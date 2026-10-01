from __future__ import annotations
from dataclasses import dataclass,field
from datetime import datetime
from hashlib import sha256
from json import dumps
from threading import RLock
from typing import Iterable,Mapping,Protocol
from .ops_store import VersionedStore
from .timeutil import utc

class HandoverError(RuntimeError):
    """A handover package violates its lifecycle rules."""

class SourceChangedDuringBuild(HandoverError):
    """A source advanced between snapshot token capture and content extraction."""

class SourceChangedBeforePublish(HandoverError):
    """A source advanced while a sealed package was waiting to be published."""


@dataclass(frozen=True)
class HandoverParty:
    company:str
    station:str
    actor:str
    def __post_init__(self):
        if not self.company or not self.station or not self.actor:
            raise ValueError("party company, station and actor are required")


@dataclass(frozen=True)
class SourceToken:
    """Opaque consistency token for one source captured at a single point in time."""
    source:str
    version:int
    fingerprint:str=""


@dataclass(frozen=True)
class HandoverItem:
    section:str
    item_id:str
    state:str
    summary:str
    source:str
    source_version:int
    detail:Mapping[str,object]=field(default_factory=dict)


@dataclass(frozen=True)
class HandoverContent:
    items:tuple[HandoverItem,...]
    tokens:Mapping[str,SourceToken]


class HandoverStatus:
    SEALED="sealed"
    PUBLISHED="published"
    SUPERSEDED="superseded"


def canonical_digest(scope:str,payload:Mapping[str,object])->str:
    raw=dumps({"scope":scope,"payload":payload},sort_keys=True,separators=(",",":"),ensure_ascii=False,default=str).encode("utf-8")
    return sha256(raw).hexdigest()


@dataclass(frozen=True)
class HandoverPackage:
    package_id:str
    flight_id:str
    aircraft_registration:str
    from_party:HandoverParty
    to_party:HandoverParty
    sealed_at:datetime
    status:str
    content:HandoverContent
    content_digest:str
    supersession:Mapping[str,str]=field(default_factory=dict)
    published_at:datetime|None=None

    def verify(self)->bool:
        return self.content_digest==_digest_content(self.flight_id,self.aircraft_registration,self.content)


def _digest_content(flight_id:str,aircraft_registration:str,content:HandoverContent)->str:
    payload={
        "flight_id":flight_id,
        "aircraft_registration":aircraft_registration,
        "tokens":{name:{"source":t.source,"version":t.version,"fingerprint":t.fingerprint} for name,t in sorted(content.tokens.items())},
        "items":[{"section":i.section,"item_id":i.item_id,"state":i.state,"summary":i.summary,
                  "source":i.source,"source_version":i.source_version,"detail":dict(i.detail)} for i in content.items],
    }
    return canonical_digest("handover.content",payload)


class HandoverSource(Protocol):
    name:str
    def current_token(self)->SourceToken:...
    def extract(self,token:SourceToken)->tuple[HandoverItem,...]:...


class VersionedStoreSource:
    """Adapts a VersionedStore to the point-in-time handover source protocol.

    The store generation pins the whole snapshot; extractors turn the frozen
    records of one generation into handover items.  Any write to the store
    bumps its generation, so a package built against generation N can never be
    published once the source has moved on.
    """
    def __init__(self,name:str,section:str,store:VersionedStore,state_get=None,summary_get=None,detail_get=None,predicate=lambda v:True):
        self.name=name;self.section=section;self._store=store
        self._state=state_get or (lambda v:str(v.get("state","unknown")))
        self._summary=summary_get or (lambda v:str(v.get("summary",v)))
        self._detail=detail_get or (lambda v:(v if isinstance(v,Mapping) else {"value":v}))
        self._predicate=predicate

    def current_token(self)->SourceToken:
        snap=self._store.snapshot()
        fingerprint=canonical_digest(f"handover.source.{self.name}",{
            k:{"version":r.version,"updated_at":r.updated_at.isoformat(),"value":r.value}
            for k,r in sorted(snap.records.items())
        })
        return SourceToken(self.name,snap.generation,fingerprint)

    def extract(self,token:SourceToken)->tuple[HandoverItem,...]:
        snap=self._store.snapshot()
        if snap.generation!=token.version:
            raise SourceChangedDuringBuild(f"source {self.name} moved from {token.version} to {snap.generation} during build")
        items=[]
        for key in sorted(snap.records):
            rec=snap.records[key]
            if not self._predicate(rec.value):continue
            items.append(HandoverItem(self.section,key,self._state(rec.value),self._summary(rec.value),
                                      self.name,rec.version,dict(self._detail(rec.value))))
        return tuple(items)


@dataclass(frozen=True)
class ItemAcknowledgment:
    item_id:str
    accepted:bool
    actor:str
    at:datetime
    note:str=""


@dataclass(frozen=True)
class PartySignoff:
    party_role:str
    company:str
    actor:str
    at:datetime


@dataclass(frozen=True)
class HandoverReceipt:
    """The receiving company's sign-off sheet. It only annotates the package."""
    package_id:str
    package_digest:str
    acknowledgments:Mapping[str,ItemAcknowledgment]
    from_signoff:PartySignoff|None
    to_signoff:PartySignoff|None
    receipt_digest:str


def _digest_receipt(package_id:str,package_digest:str,acks:Mapping[str,ItemAcknowledgment],from_signoff:PartySignoff|None,to_signoff:PartySignoff|None)->str:
    def sig(s):
        return None if s is None else {"party_role":s.party_role,"company":s.company,"actor":s.actor,"at":s.at.isoformat()}
    payload={
        "package_id":package_id,"package_digest":package_digest,
        "acknowledgments":{k:{"item_id":a.item_id,"accepted":a.accepted,"actor":a.actor,"at":a.at.isoformat(),"note":a.note}
                           for k,a in sorted(acks.items())},
        "from_signoff":sig(from_signoff),"to_signoff":sig(to_signoff),
    }
    return canonical_digest("handover.receipt",payload)


class HandoverService:
    """Builds immutable point-in-time handover packages and records sign-offs.

    A package's content is read exactly once, against source tokens captured in
    one instant; the sources are revalidated before publication, so a source
    that changes during packaging aborts the release instead of producing a
    mixed snapshot.  Acknowledgments live on a separate receipt and never touch
    either the sealed package or the underlying business stores.
    """
    def __init__(self):
        self._lock=RLock()
        self._packages:dict[str,HandoverPackage]={}
        self._receipts:dict[str,HandoverReceipt]={}
        self._flight_latest:dict[str,str]={}

    def packages(self)->tuple[HandoverPackage,...]:
        with self._lock:return tuple(self._packages[k] for k in sorted(self._packages))

    def get(self,package_id:str)->HandoverPackage:
        with self._lock:
            try:return self._packages[package_id]
            except KeyError:raise KeyError(f"unknown handover package {package_id}") from None

    def receipt(self,package_id:str)->HandoverReceipt:
        with self._lock:
            try:return self._receipts[package_id]
            except KeyError:raise KeyError(f"no receipt for handover package {package_id}") from None

    def build(self,package_id:str,flight_id:str,aircraft_registration:str,
              from_party:HandoverParty,to_party:HandoverParty,sources:Iterable[HandoverSource],at:datetime,
              supersedes:str|None=None)->HandoverPackage:
        at=utc(at)
        if from_party.company==to_party.company:
            raise ValueError("handover requires two different companies")
        source_list=tuple(sources)
        names=[s.name for s in source_list]
        if len(set(names))!=len(names):raise ValueError("duplicate handover source names")
        with self._lock:
            if package_id in self._packages:raise HandoverError(f"package {package_id} already exists")
            if supersedes is not None and supersedes not in self._packages:
                raise HandoverError(f"cannot supersede unknown package {supersedes}")
            # Phase 1: pin every source at the same instant.
            tokens={s.name:s.current_token() for s in source_list}
            # Phase 2: extract under the pins. A moved source raises here.
            items:list[HandoverItem]=[]
            for s in source_list:items.extend(s.extract(tokens[s.name]))
            content=HandoverContent(tuple(items),dict(tokens))
            pkg=HandoverPackage(package_id,flight_id,aircraft_registration,from_party,to_party,
                                at,HandoverStatus.SEALED,content,_digest_content(flight_id,aircraft_registration,content),
                                {"supersedes":supersedes} if supersedes else {})
            self._packages[package_id]=pkg
            return pkg

    def publish(self,package_id:str,sources:Iterable[HandoverSource],at:datetime)->HandoverPackage:
        at=utc(at);source_list=tuple(sources)
        with self._lock:
            pkg=self._packages.get(package_id)
            if pkg is None:raise HandoverError(f"unknown package {package_id}")
            if pkg.status==HandoverStatus.PUBLISHED:raise HandoverError("package already published")
            if pkg.status==HandoverStatus.SUPERSEDED:raise HandoverError("package superseded")
            by_name={s.name:s for s in source_list}
            missing=set(pkg.content.tokens)-set(by_name)
            if missing:raise HandoverError(f"missing sources at publish: {sorted(missing)}")
            for name,token in pkg.content.tokens.items():
                current=by_name[name].current_token()
                if current.version!=token.version or current.fingerprint!=token.fingerprint:
                    raise SourceChangedBeforePublish(f"source {name} changed while package {package_id} was sealed")
            previous=pkg.supersession.get("supersedes")
            if previous is not None:
                old=self._packages[previous]
                if old.status not in {HandoverStatus.SEALED,HandoverStatus.PUBLISHED}:
                    raise HandoverError("cannot supersede a package that is already superseded")
                self._packages[previous]=_replace_status(old,HandoverStatus.SUPERSEDED)
            published=_replace_published(pkg,at)
            self._packages[package_id]=published
            self._flight_latest[pkg.flight_id]=package_id
            self._receipts.setdefault(package_id,_fresh_receipt(published))
            return published

    def latest_for_flight(self,flight_id:str)->HandoverPackage:
        with self._lock:
            pid=self._flight_latest.get(flight_id)
            if pid is None:raise KeyError(f"no published handover for {flight_id}")
            return self._packages[pid]

    def acknowledge_item(self,package_id:str,item_id:str,accepted:bool,actor:str,at:datetime,note:str="")->HandoverReceipt:
        at=utc(at)
        if not actor:raise ValueError("acknowledgment actor required")
        with self._lock:
            pkg=self._require_published(package_id)
            if actor!=pkg.to_party.actor:
                raise HandoverError("only the receiving party signs off items")
            ids={i.item_id for i in pkg.content.items}
            if item_id not in ids:raise HandoverError(f"item {item_id} is not part of package {package_id}")
            if not accepted and not note:raise ValueError("a disputed item requires a note")
            receipt=self._receipts[package_id]
            if receipt.to_signoff is not None:raise HandoverError("receipt already closed")
            acks=dict(receipt.acknowledgments)
            acks[item_id]=ItemAcknowledgment(item_id,accepted,actor,at,note)
            updated=_replace_acks(receipt,acks,pkg)
            self._receipts[package_id]=updated
            return updated

    def acknowledge_all(self,package_id:str,accepted:bool,actor:str,at:datetime,note:str="")->HandoverReceipt:
        with self._lock:
            pkg=self._require_published(package_id)
            for item in pkg.content.items:
                self.acknowledge_item(package_id,item.item_id,accepted,actor,at,note)
            return self._receipts[package_id]

    def sign_off(self,package_id:str,party_role:str,actor:str,at:datetime)->HandoverReceipt:
        at=utc(at)
        with self._lock:
            pkg=self._require_published(package_id)
            if party_role=="from":expected=pkg.from_party.company
            elif party_role=="to":expected=pkg.to_party.company
            else:raise ValueError("party_role must be 'from' or 'to'")
            receipt=self._receipts[package_id]
            if party_role=="from":
                if actor!=pkg.from_party.actor:raise HandoverError("only the handing-over party signs as 'from'")
                if receipt.from_signoff is not None:raise HandoverError("from party already signed")
                signoff=PartySignoff("from",expected,actor,at)
                updated=HandoverReceipt(receipt.package_id,receipt.package_digest,dict(receipt.acknowledgments),
                                        signoff,receipt.to_signoff,_digest_receipt(package_id,receipt.package_digest,
                                                                                  receipt.acknowledgments,signoff,receipt.to_signoff))
            else:
                if actor!=pkg.to_party.actor:raise HandoverError("only the receiving party signs as 'to'")
                if receipt.to_signoff is not None:raise HandoverError("to party already signed")
                pending={i.item_id for i in pkg.content.items if i.item_id not in receipt.acknowledgments}
                if pending:raise HandoverError(f"items not acknowledged: {sorted(pending)}")
                signoff=PartySignoff("to",expected,actor,at)
                updated=HandoverReceipt(receipt.package_id,receipt.package_digest,dict(receipt.acknowledgments),
                                        receipt.from_signoff,signoff,_digest_receipt(package_id,receipt.package_digest,
                                                                                    receipt.acknowledgments,receipt.from_signoff,signoff))
            self._receipts[package_id]=updated
            return updated

    def is_complete(self,package_id:str)->bool:
        with self._lock:
            r=self._receipts[package_id]
            return r.from_signoff is not None and r.to_signoff is not None

    def _require_published(self,package_id:str)->HandoverPackage:
        pkg=self._packages.get(package_id)
        if pkg is None:raise HandoverError(f"unknown package {package_id}")
        if pkg.status!=HandoverStatus.PUBLISHED:raise HandoverError("package not published")
        return pkg


def _replace_status(pkg:HandoverPackage,status:str)->HandoverPackage:
    return HandoverPackage(pkg.package_id,pkg.flight_id,pkg.aircraft_registration,pkg.from_party,pkg.to_party,
                           pkg.sealed_at,status,pkg.content,pkg.content_digest,pkg.supersession,pkg.published_at)

def _replace_published(pkg:HandoverPackage,at:datetime)->HandoverPackage:
    return HandoverPackage(pkg.package_id,pkg.flight_id,pkg.aircraft_registration,pkg.from_party,pkg.to_party,
                           pkg.sealed_at,HandoverStatus.PUBLISHED,pkg.content,pkg.content_digest,pkg.supersession,at)

def _fresh_receipt(pkg:HandoverPackage)->HandoverReceipt:
    return HandoverReceipt(pkg.package_id,pkg.content_digest,{},None,None,
                           _digest_receipt(pkg.package_id,pkg.content_digest,{},None,None))

def _replace_acks(receipt:HandoverReceipt,acks:Mapping[str,ItemAcknowledgment],pkg:HandoverPackage)->HandoverReceipt:
    return HandoverReceipt(receipt.package_id,receipt.package_digest,acks,receipt.from_signoff,receipt.to_signoff,
                           _digest_receipt(receipt.package_id,pkg.content_digest,acks,receipt.from_signoff,receipt.to_signoff))
