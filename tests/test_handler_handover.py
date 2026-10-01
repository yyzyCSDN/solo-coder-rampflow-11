import unittest
from dataclasses import replace
from datetime import datetime, timezone

from rampflow.handler_handover import (
    GroundHandlerHandoverService,
    HandoverAcknowledgementLedger,
    HandoverItemCategory,
    HandoverSourceChanged,
    InvalidHandoverPackage,
    OccupiedEquipment,
    PendingTask,
    SiteAnomaly,
    HandoverPackageCodec,
    verify_handover_package,
)
from rampflow.models import ServiceState
from rampflow.ops_store import VersionedStore


UTC = timezone.utc
D = lambda h, m=0: datetime(2026, 1, 1, h, m, tzinfo=UTC)


class MutationDuringSealStore(VersionedStore):
    def __init__(self, value, actor: str, at):
        super().__init__()
        self._value = value
        self._actor = actor
        self._at = at
        self.create("task-1", value, actor, "before seal", at)
        self._reads = 0

    def snapshot(self):
        result = super().snapshot()
        self._reads += 1
        if self._reads == 1:
            self.put("task-1", self._changed_value(), 1, self._actor, "changed during seal", self._at)
        return result

    def _changed_value(self):
        raise NotImplementedError


class HandlerHandoverTests(unittest.TestCase):
    def setUp(self):
        self.service = GroundHandlerHandoverService()
        self.service.tasks.create(
            "fuel",
            PendingTask(
                "fuel",
                "Refuel aircraft",
                ServiceState.ACTIVE,
                D(10, 30),
                frozenset({"GPU-1"}),
            ),
            "old-co",
            "work in progress",
            D(10),
        )
        self.service.occupied_equipment.create(
            "GPU-1",
            OccupiedEquipment("GPU-1", "gpu", "stand-12", "old-co", D(9, 50)),
            "old-co",
            "equipment in use",
            D(9, 50),
        )
        self.service.site_anomalies.create(
            "fuel-spill",
            SiteAnomaly("fuel-spill", "medium", "stand-12", "Small fuel spill coned off", D(9, 55)),
            "old-co",
            "anomaly reported",
            D(9, 55),
        )

    def test_sealed_package_captures_all_sources_at_one_point(self):
        package = self.service.seal_handover("H-1", "CA123", "old-co", "new-co", D(10))

        self.assertTrue(verify_handover_package(package))
        encoded = HandoverPackageCodec().encode(package)
        self.assertEqual(HandoverPackageCodec().decode(encoded), package)
        self.assertEqual(
            package.source_generations,
            {"occupied_equipment": 1, "site_anomalies": 1, "tasks": 1},
        )
        self.assertEqual(
            tuple(item.category for item in package.items),
            (
                HandoverItemCategory.PENDING_TASK.value,
                HandoverItemCategory.OCCUPIED_EQUIPMENT.value,
                HandoverItemCategory.SITE_ANOMALY.value,
            ),
        )

        self.service.tasks.put(
            "fuel",
            PendingTask(
                "fuel",
                "Refuel aircraft",
                ServiceState.BLOCKED,
                D(10, 45),
                frozenset({"GPU-1"}),
                "waiting on bowser",
            ),
            1,
            "new-co",
            "post-seal operational update",
            D(10, 5),
        )
        reread = self.service.package("H-1")
        self.assertEqual(reread, package)
        self.assertTrue(verify_handover_package(reread))
        self.assertEqual(
            reread.items[0].details["state"],
            ServiceState.ACTIVE,
        )

    def test_change_while_reading_sources_aborts_without_publishing(self):
        service = GroundHandlerHandoverService()
        equipment = OccupiedEquipment("GPU-1", "gpu", "stand-12", "old-co", D(9, 50))
        service.occupied_equipment = MutationDuringSealStore(equipment, "old-co", D(9, 55))
        service.occupied_equipment._changed_value = lambda: replace(equipment, zone="stand-13")

        with self.assertRaises(HandoverSourceChanged):
            service.seal_handover("H-2", "CA123", "old-co", "new-co", D(10))

        self.assertEqual(service.packages(), ())

    def test_item_acknowledgement_is_only_receipt_and_cannot_change_package(self):
        package = self.service.seal_handover("H-3", "CA123", "old-co", "new-co", D(10))
        at = D(10, 1)

        receipt = self.service.acknowledge_item("H-3", "new-co", "task:fuel", at)
        duplicate = self.service.acknowledge_item("H-3", "new-co", "task:fuel", at)

        self.assertEqual(receipt, duplicate)
        self.assertEqual(receipt.package_digest, package.digest)
        self.assertEqual(receipt.role, "incoming")
        self.assertEqual(
            self.service.acknowledgements.missing_items(package, "new-co"),
            ("equipment:GPU-1", "anomaly:fuel-spill"),
        )
        self.assertFalse(self.service.handover_complete("H-3"))
        self.assertEqual(self.service.package("H-3"), package)

        for item_id in ("equipment:GPU-1", "anomaly:fuel-spill"):
            self.service.acknowledge_item("H-3", "new-co", item_id, at)
        self.service.sign_package("H-3", "old-co", at)
        self.assertFalse(self.service.handover_complete("H-3"))
        self.service.sign_package("H-3", "new-co", at)

        self.assertTrue(self.service.handover_complete("H-3"))

    def test_unknown_or_tampered_package_cannot_be_signed(self):
        package = self.service.seal_handover("H-4", "CA123", "old-co", "new-co", D(10))
        ledger = HandoverAcknowledgementLedger()

        with self.assertRaises(KeyError):
            ledger.acknowledge_item(package, "new-co", "task:missing", D(10, 1))
        with self.assertRaises(ValueError):
            ledger.acknowledge_item(package, "third-co", "task:fuel", D(10, 1))
        with self.assertRaises(ValueError):
            ledger.acknowledge_item(package, "old-co", "task:fuel", D(10, 1))

        tampered_item = replace(package.items[0], summary="changed after sealing")
        tampered = replace(package, items=(tampered_item, *package.items[1:]))
        self.assertFalse(verify_handover_package(tampered))
        with self.assertRaises(InvalidHandoverPackage):
            ledger.acknowledge_item(tampered, "new-co", tampered_item.item_id, D(10, 1))


if __name__ == "__main__":
    unittest.main()
