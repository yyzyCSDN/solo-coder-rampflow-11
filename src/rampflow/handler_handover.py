from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum
from hashlib import sha256
from json import dumps, loads
from threading import RLock
from types import MappingProxyType
from typing import Any, Mapping

from .models import ServiceState
from .ops_store import Snapshot, Versioned, VersionedStore
from .timeutil import utc


TERMINAL_TASK_STATES = frozenset({ServiceState.COMPLETE, ServiceState.CANCELLED})


class HandoverItemCategory(str, Enum):
    PENDING_TASK = "pending_task"
    OCCUPIED_EQUIPMENT = "occupied_equipment"
    SITE_ANOMALY = "site_anomaly"


class HandoverSourceChanged(RuntimeError):
    """Raised when an operational source changes during package sealing."""


class InvalidHandoverPackage(ValueError):
    """Raised when a handover package or acknowledgement fails integrity checks."""


@dataclass(frozen=True)
class PendingTask:
    task_id: str
    name: str
    state: ServiceState
    planned_end: datetime
    assigned_equipment: frozenset[str] = frozenset()
    blocked_reason: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "planned_end", utc(self.planned_end))
        if not self.task_id or not self.name:
            raise ValueError("task identity and name are required")
        if self.state in TERMINAL_TASK_STATES:
            raise ValueError("only unfinished tasks belong in a handler handover")


@dataclass(frozen=True)
class OccupiedEquipment:
    equipment_id: str
    kind: str
    zone: str
    held_by: str
    occupied_from: datetime
    exclusive: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "occupied_from", utc(self.occupied_from))
        if not self.equipment_id or not self.kind or not self.held_by:
            raise ValueError("equipment identity, kind and holder are required")


@dataclass(frozen=True)
class SiteAnomaly:
    anomaly_id: str
    severity: str
    location: str
    summary: str
    opened_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "opened_at", utc(self.opened_at))
        if not self.anomaly_id or not self.severity or not self.summary:
            raise ValueError("anomaly identity, severity and summary are required")


@dataclass(frozen=True)
class HandoverItem:
    item_id: str
    category: str
    source_name: str
    source_key: str
    source_version: int
    summary: str
    details: Mapping[str, object]

    def __post_init__(self) -> None:
        object.__setattr__(self, "details", MappingProxyType(dict(self.details)))


@dataclass(frozen=True)
class HandoverPackage:
    package_id: str
    flight_id: str
    sealed_at: datetime
    outgoing_company: str
    incoming_company: str
    items: tuple[HandoverItem, ...]
    source_generations: Mapping[str, int]
    digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "sealed_at", utc(self.sealed_at))
        object.__setattr__(self, "source_generations", MappingProxyType(dict(self.source_generations)))


@dataclass(frozen=True)
class HandoverAcknowledgement:
    package_id: str
    package_digest: str
    item_id: str
    party: str
    role: str
    acknowledged_at: datetime


@dataclass(frozen=True)
class HandoverSignature:
    package_id: str
    package_digest: str
    party: str
    role: str
    signed_at: datetime


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, frozenset):
        return sorted(value, key=str)
    raise TypeError(f"object of type {type(value).__name__} is not handover serializable")


def _canonical_body(package: HandoverPackage) -> Mapping[str, object]:
    return {
        "package_id": package.package_id,
        "flight_id": package.flight_id,
        "sealed_at": package.sealed_at.isoformat(),
        "outgoing_company": package.outgoing_company,
        "incoming_company": package.incoming_company,
        "source_generations": dict(sorted(package.source_generations.items())),
        "items": [
            {
                "item_id": item.item_id,
                "category": item.category,
                "source_name": item.source_name,
                "source_key": item.source_key,
                "source_version": item.source_version,
                "summary": item.summary,
                "details": dict(item.details),
            }
            for item in sorted(package.items, key=lambda item: item.item_id)
        ],
    }


def handover_digest(package: HandoverPackage) -> str:
    raw = dumps(
        _canonical_body(package),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    ).encode("utf-8")
    return sha256(raw).hexdigest()


def verify_handover_package(package: HandoverPackage) -> bool:
    return handover_digest(package) == package.digest


class HandoverPackageCodec:
    @staticmethod
    def _package_json(package: HandoverPackage) -> dict[str, object]:
        return {
            "package_id": package.package_id,
            "flight_id": package.flight_id,
            "sealed_at": package.sealed_at.isoformat(),
            "outgoing_company": package.outgoing_company,
            "incoming_company": package.incoming_company,
            "source_generations": dict(package.source_generations),
            "digest": package.digest,
            "items": [
                {
                    "item_id": item.item_id,
                    "category": item.category,
                    "source_name": item.source_name,
                    "source_key": item.source_key,
                    "source_version": item.source_version,
                    "summary": item.summary,
                    "details": dict(item.details),
                }
                for item in package.items
            ],
        }

    @staticmethod
    def _datetime_value(mapping: Mapping[str, object], key: str) -> datetime:
        return utc(datetime.fromisoformat(str(mapping[key])))

    def encode(self, package: HandoverPackage) -> bytes:
        if not verify_handover_package(package):
            raise InvalidHandoverPackage("handover package digest mismatch")
        return dumps(
            self._package_json(package),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=_json_default,
        ).encode("utf-8")

    def decode(self, data: bytes) -> HandoverPackage:
        body = loads(data.decode("utf-8"))
        items = []
        for raw_item in body["items"]:
            details = dict(raw_item["details"])
            for key, value in tuple(details.items()):
                if key in {"planned_end", "occupied_from", "opened_at"}:
                    details[key] = datetime.fromisoformat(str(value))
                elif key == "state":
                    details[key] = ServiceState(value)
                elif key == "assigned_equipment":
                    details[key] = tuple(value)
            items.append(
                HandoverItem(
                    raw_item["item_id"],
                    raw_item["category"],
                    raw_item["source_name"],
                    raw_item["source_key"],
                    int(raw_item["source_version"]),
                    raw_item["summary"],
                    details,
                )
            )
        package = HandoverPackage(
            body["package_id"],
            body["flight_id"],
            self._datetime_value(body, "sealed_at"),
            body["outgoing_company"],
            body["incoming_company"],
            tuple(items),
            {str(key): int(value) for key, value in body["source_generations"].items()},
            body["digest"],
        )
        if not verify_handover_package(package):
            raise InvalidHandoverPackage("handover package digest mismatch")
        return package


def _pending_task_item(record: Versioned[PendingTask]) -> HandoverItem:
    value = record.value
    details = {
        "task_id": value.task_id,
        "name": value.name,
        "state": value.state,
        "planned_end": value.planned_end,
        "assigned_equipment": tuple(sorted(value.assigned_equipment)),
        "blocked_reason": value.blocked_reason,
    }
    return HandoverItem(
        f"task:{record.key}",
        HandoverItemCategory.PENDING_TASK.value,
        "tasks",
        record.key,
        record.version,
        f"Pending task {value.task_id} is {value.state.value}: {value.name}",
        details,
    )


def _occupied_equipment_item(record: Versioned[OccupiedEquipment]) -> HandoverItem:
    value = record.value
    details = {
        "equipment_id": value.equipment_id,
        "kind": value.kind,
        "zone": value.zone,
        "held_by": value.held_by,
        "occupied_from": value.occupied_from,
        "exclusive": value.exclusive,
    }
    return HandoverItem(
        f"equipment:{record.key}",
        HandoverItemCategory.OCCUPIED_EQUIPMENT.value,
        "occupied_equipment",
        record.key,
        record.version,
        f"Equipment {value.equipment_id} is held by {value.held_by} in {value.zone}",
        details,
    )


def _site_anomaly_item(record: Versioned[SiteAnomaly]) -> HandoverItem:
    value = record.value
    details = {
        "anomaly_id": value.anomaly_id,
        "severity": value.severity,
        "location": value.location,
        "summary": value.summary,
        "opened_at": value.opened_at,
    }
    return HandoverItem(
        f"anomaly:{record.key}",
        HandoverItemCategory.SITE_ANOMALY.value,
        "site_anomalies",
        record.key,
        record.version,
        f"{value.severity} anomaly at {value.location}: {value.summary}",
        details,
    )


class HandoverPackageBuilder:
    """Builds one immutable package from an optimistic atomic read of all sources."""

    def __init__(
        self,
        tasks: VersionedStore[PendingTask],
        occupied_equipment: VersionedStore[OccupiedEquipment],
        site_anomalies: VersionedStore[SiteAnomaly],
    ) -> None:
        self._sources = (
            ("tasks", tasks, _pending_task_item),
            ("occupied_equipment", occupied_equipment, _occupied_equipment_item),
            ("site_anomalies", site_anomalies, _site_anomaly_item),
        )

    def build(
        self,
        package_id: str,
        flight_id: str,
        outgoing_company: str,
        incoming_company: str,
        at: datetime,
    ) -> HandoverPackage:
        if not package_id or not flight_id:
            raise ValueError("package and flight identities are required")
        if not outgoing_company or not incoming_company:
            raise ValueError("both handing companies are required")
        if outgoing_company == incoming_company:
            raise ValueError("outgoing and incoming companies must differ")

        sealed_at = utc(at)
        snapshots: tuple[tuple[str, VersionedStore[Any], Snapshot[Any]], ...] = tuple(
            (name, store, store.snapshot()) for name, store, _ in self._sources
        )

        # Each snapshot is atomic for its store.  Checking every generation after
        # every read establishes one optimistic serialization point: a change to
        # an earlier source while a later source is read invalidates the package.
        for name, store, snapshot in snapshots:
            if store.generation != snapshot.generation:
                raise HandoverSourceChanged(
                    f"{name} changed while handler handover {package_id} was being sealed"
                )

        for _, _, snapshot in snapshots:
            for record in snapshot.records.values():
                if record.updated_at > sealed_at:
                    raise HandoverSourceChanged(
                        f"source record {record.key} is newer than handover point {sealed_at.isoformat()}"
                    )

        items: list[HandoverItem] = []
        source_generations: dict[str, int] = {}
        for name, _, snapshot in snapshots:
            source_generations[name] = snapshot.generation
            converter = next(converter for source_name, _, converter in self._sources if source_name == name)
            items.extend(converter(record) for record in sorted(snapshot.records.values(), key=lambda r: r.key))

        unsigned = HandoverPackage(
            package_id,
            flight_id,
            sealed_at,
            outgoing_company,
            incoming_company,
            tuple(items),
            dict(sorted(source_generations.items())),
            "",
        )
        return replace(unsigned, digest=handover_digest(unsigned))


class HandoverAcknowledgementLedger:
    """Item receipts kept separately from the immutable operational package."""

    def __init__(self) -> None:
        self._receipts: dict[tuple[str, str, str], HandoverAcknowledgement] = {}
        self._signatures: dict[tuple[str, str], HandoverSignature] = {}
        self._lock = RLock()

    @staticmethod
    def _participant(package: HandoverPackage, party: str) -> str:
        if party == package.outgoing_company:
            return "outgoing"
        if party == package.incoming_company:
            return "incoming"
        raise ValueError("party is not a participant in this handover")

    def acknowledge_item(
        self,
        package: HandoverPackage,
        party: str,
        item_id: str,
        at: datetime,
    ) -> HandoverAcknowledgement:
        if not verify_handover_package(package):
            raise InvalidHandoverPackage("handover package digest mismatch")
        role = self._participant(package, party)
        if role != "incoming":
            raise ValueError("only the incoming company signs an item receipt")
        if not any(item.item_id == item_id for item in package.items):
            raise KeyError(item_id)

        acknowledged_at = utc(at)
        if acknowledged_at < package.sealed_at:
            raise ValueError("cannot acknowledge a package before it was sealed")

        key = (package.package_id, party, item_id)
        with self._lock:
            existing = self._receipts.get(key)
            if existing is not None:
                return existing
            receipt = HandoverAcknowledgement(
                package.package_id,
                package.digest,
                item_id,
                party,
                role,
                acknowledged_at,
            )
            self._receipts[key] = receipt
            return receipt

    def sign_package(
        self,
        package: HandoverPackage,
        party: str,
        at: datetime,
    ) -> HandoverSignature:
        if not verify_handover_package(package):
            raise InvalidHandoverPackage("handover package digest mismatch")
        role = self._participant(package, party)
        signed_at = utc(at)
        if signed_at < package.sealed_at:
            raise ValueError("cannot sign a package before it was sealed")

        key = (package.package_id, party)
        with self._lock:
            existing = self._signatures.get(key)
            if existing is not None:
                return existing
            signature = HandoverSignature(package.package_id, package.digest, party, role, signed_at)
            self._signatures[key] = signature
            return signature

    def for_package(self, package_id: str, party: str | None = None) -> tuple[HandoverAcknowledgement, ...]:
        with self._lock:
            return tuple(
                receipt
                for receipt in self._receipts.values()
                if receipt.package_id == package_id and (party is None or receipt.party == party)
            )

    def signatures(self, package_id: str, party: str | None = None) -> tuple[HandoverSignature, ...]:
        with self._lock:
            return tuple(
                signature
                for signature in self._signatures.values()
                if signature.package_id == package_id and (party is None or signature.party == party)
            )

    def missing_items(self, package: HandoverPackage, party: str) -> tuple[str, ...]:
        acknowledged = {
            receipt.item_id
            for receipt in self.for_package(package.package_id, party)
            if receipt.package_digest == package.digest
        }
        return tuple(item.item_id for item in package.items if item.item_id not in acknowledged)

    def is_item_receipt_complete(self, package: HandoverPackage, party: str) -> bool:
        role = self._participant(package, party)
        if role != "incoming":
            raise ValueError("item receipts are completed by the incoming company")
        return not self.missing_items(package, party)

    def is_signed_by(self, package: HandoverPackage, party: str) -> bool:
        self._participant(package, party)
        key = (package.package_id, party)
        with self._lock:
            signature = self._signatures.get(key)
            return signature is not None and signature.package_digest == package.digest


class GroundHandlerHandoverService:
    """Source repositories, sealed packages and item-level sign-off receipts."""

    def __init__(self) -> None:
        self.tasks = VersionedStore[PendingTask]()
        self.occupied_equipment = VersionedStore[OccupiedEquipment]()
        self.site_anomalies = VersionedStore[SiteAnomaly]()
        self.acknowledgements = HandoverAcknowledgementLedger()
        self._packages: dict[str, HandoverPackage] = {}
        self._lock = RLock()

    def seal_handover(
        self,
        package_id: str,
        flight_id: str,
        outgoing_company: str,
        incoming_company: str,
        at: datetime,
    ) -> HandoverPackage:
        with self._lock:
            if package_id in self._packages:
                raise ValueError(f"duplicate handover package {package_id}")
            package = HandoverPackageBuilder(
                self.tasks,
                self.occupied_equipment,
                self.site_anomalies,
            ).build(package_id, flight_id, outgoing_company, incoming_company, at)
            self._packages[package_id] = package
            return package

    def package(self, package_id: str) -> HandoverPackage:
        return self._packages[package_id]

    def packages(self) -> tuple[HandoverPackage, ...]:
        with self._lock:
            return tuple(self._packages[key] for key in sorted(self._packages))

    def acknowledge_item(
        self,
        package_id: str,
        party: str,
        item_id: str,
        at: datetime,
    ) -> HandoverAcknowledgement:
        return self.acknowledgements.acknowledge_item(self.package(package_id), party, item_id, at)

    def sign_package(self, package_id: str, party: str, at: datetime) -> HandoverSignature:
        return self.acknowledgements.sign_package(self.package(package_id), party, at)

    def handover_complete(self, package_id: str) -> bool:
        package = self.package(package_id)
        return (
            self.acknowledgements.is_item_receipt_complete(package, package.incoming_company)
            and self.acknowledgements.is_signed_by(package, package.outgoing_company)
            and self.acknowledgements.is_signed_by(package, package.incoming_company)
        )
