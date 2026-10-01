import unittest
from datetime import datetime, timezone

from rampflow.ops_store import VersionedStore
from rampflow.handover import (
    HandoverService, HandoverParty, VersionedStoreSource,
    SourceChangedBeforePublish, HandoverError,
)
from rampflow.workflows.control import RampWorkflowControl


UTC = timezone.utc
D = lambda h, m=0: datetime(2026, 1, 1, h, m, tzinfo=UTC)

OPEN_TASK = {"state": "active", "summary": "fueling in progress"}
OPEN_INCIDENT = {"state": "open", "summary": "GPU fault near stand 12"}


def _stores():
    tasks = VersionedStore()
    tasks.create("T-fuel", OPEN_TASK, "A-ops", "turnaround", D(8))
    equipment = VersionedStore()
    equipment.create("E-gpu1", {"state": "occupied", "summary": "GPU1 connected"}, "A-ops", "dispatch", D(8, 5))
    incidents = VersionedStore()
    incidents.create("I-gpu", OPEN_INCIDENT, "A-ops", "incident", D(8, 10))
    return tasks, equipment, incidents


def _sources(tasks, equipment, incidents):
    return (
        VersionedStoreSource("tasks", "open_tasks", tasks),
        VersionedStoreSource("equipment", "occupied_equipment", equipment),
        VersionedStoreSource("incidents", "site_anomalies", incidents),
    )


def _parties():
    return (
        HandoverParty("Alpha Ground", "PVG-T2", "alice"),
        HandoverParty("Bravo Ramp", "PVG-T2", "bob"),
    )


def _build_and_publish(service, sources, at=D(9), package_id="H1"):
    frm, to = _parties()
    pkg = service.build(package_id, "MU5101", "B-2035", frm, to, sources, at)
    return service.publish(package_id, sources, at)


class HandoverPackageTests(unittest.TestCase):
    def test_package_captures_all_sections_at_one_point(self):
        tasks, equipment, incidents = _stores()
        sources = _sources(tasks, equipment, incidents)
        service = HandoverService()
        pkg = _build_and_publish(service, sources)

        sections = {}
        versions = {}
        for item in pkg.content.items:
            sections.setdefault(item.section, []).append(item.item_id)
            versions[item.source] = pkg.content.tokens[item.source].version
        self.assertEqual(set(sections), {"open_tasks", "occupied_equipment", "site_anomalies"})
        self.assertTrue(pkg.verify())
        self.assertEqual(pkg.status, "published")
        self.assertEqual(pkg.published_at, D(9))
        self.assertEqual(set(versions), {"tasks", "equipment", "incidents"})

    def test_source_change_before_publish_aborts_release(self):
        tasks, equipment, incidents = _stores()
        sources = _sources(tasks, equipment, incidents)
        service = HandoverService()
        frm, to = _parties()
        pkg = service.build("H1", "MU5101", "B-2035", frm, to, sources, D(9))

        # A writer finishes the fueling task while the package is sealed.
        tasks.put("T-fuel", {"state": "complete", "summary": "fueling done"}, 1, "A-ops", "task complete", D(9, 1))

        with self.assertRaises(SourceChangedBeforePublish):
            service.publish("H1", sources, D(9, 2))
        sealed = service.get("H1")
        self.assertEqual(sealed.status, "sealed")
        # The old content is untouched: no mixed snapshot is published.
        self.assertEqual(
            [i.state for i in sealed.content.items if i.item_id == "T-fuel"], ["active"]
        )

        # Rebuild against the new point in time, then publish succeeds.
        pkg2 = service.build("H2", "MU5101", "B-2035", frm, to, sources, D(9, 3))
        published = service.publish("H2", sources, D(9, 4))
        self.assertEqual(
            [i.state for i in published.content.items if i.item_id == "T-fuel"], ["complete"]
        )

    def test_acknowledgment_never_rewrites_business_records(self):
        tasks, equipment, incidents = _stores()
        sources = _sources(tasks, equipment, incidents)
        service = HandoverService()
        pkg = _build_and_publish(service, sources)
        before = tasks.snapshot()
        digest_before = pkg.content_digest

        service.acknowledge_item("H1", "T-fuel", True, "bob", D(9, 5))
        service.acknowledge_item("H1", "E-gpu1", True, "bob", D(9, 6))
        service.acknowledge_item(
            "H1", "I-gpu", False, "bob", D(9, 7), note="fault indicator still lit, need logbook"
        )

        # Source stores are byte-for-byte unchanged; package content is frozen.
        self.assertEqual(tasks.snapshot().generation, before.generation)
        self.assertEqual(tasks.get("T-fuel").value, OPEN_TASK)
        self.assertEqual(incidents.get("I-gpu").value, OPEN_INCIDENT)
        self.assertEqual(service.get("H1").content_digest, digest_before)

        receipt = service.receipt("H1")
        self.assertFalse(receipt.acknowledgments["I-gpu"].accepted)
        # The receipt is anchored to the exact package content it annotates.
        self.assertEqual(receipt.package_digest, digest_before)

    def test_dispute_without_note_and_foreign_actor_rejected(self):
        tasks, equipment, incidents = _stores()
        sources = _sources(tasks, equipment, incidents)
        service = HandoverService()
        _build_and_publish(service, sources)
        with self.assertRaises(ValueError):
            service.acknowledge_item("H1", "T-fuel", False, "bob", D(9, 5))
        with self.assertRaises(HandoverError):
            service.acknowledge_item("H1", "T-fuel", True, "alice", D(9, 5))
        with self.assertRaises(HandoverError):
            service.acknowledge_item("H1", "UNKNOWN", True, "bob", D(9, 5))

    def test_both_parties_sign_after_item_level_confirmation(self):
        tasks, equipment, incidents = _stores()
        sources = _sources(tasks, equipment, incidents)
        service = HandoverService()
        _build_and_publish(service, sources)

        # Receiver cannot close until every item is confirmed.
        with self.assertRaises(HandoverError):
            service.sign_off("H1", "to", "bob", D(9, 30))

        service.acknowledge_all("H1", True, "bob", D(9, 20))
        service.sign_off("H1", "to", "bob", D(9, 30))
        self.assertFalse(service.is_complete("H1"))
        service.sign_off("H1", "from", "alice", D(9, 31))
        self.assertTrue(service.is_complete("H1"))

        # A closed receipt rejects further item edits.
        with self.assertRaises(HandoverError):
            service.acknowledge_item("H1", "T-fuel", False, "bob", D(9, 40), note="late dispute")

    def test_sealed_package_is_not_handed_out_yet(self):
        tasks, equipment, incidents = _stores()
        sources = _sources(tasks, equipment, incidents)
        service = HandoverService()
        frm, to = _parties()
        service.build("H1", "MU5101", "B-2035", frm, to, sources, D(9))
        with self.assertRaises(HandoverError):
            service.acknowledge_item("H1", "T-fuel", True, "bob", D(9, 5))
        with self.assertRaises(KeyError):
            service.latest_for_flight("MU5101")

    def test_supersession_marks_old_package(self):
        tasks, equipment, incidents = _stores()
        sources = _sources(tasks, equipment, incidents)
        service = HandoverService()
        first = _build_and_publish(service, sources)
        equipment.put("E-gpu1", {"state": "released", "summary": "GPU1 disconnected"}, 1, "A-ops", "release", D(10))
        frm, to = _parties()
        rebuilt = service.build("H2", "MU5101", "B-2035", frm, to, sources, D(10, 1), supersedes="H1")
        second = service.publish("H2", sources, D(10, 2))
        self.assertEqual(service.get("H1").status, "superseded")
        self.assertEqual(service.latest_for_flight("MU5101").package_id, "H2")
        self.assertNotEqual(first.content_digest, second.content_digest)
        self.assertTrue(second.verify())


class FlakySource:
    """Source whose generation advances between token capture and extraction."""
    name = "tasks"

    def __init__(self, store):
        self._store = store
        self._calls = 0

    def current_token(self):
        return VersionedStoreSource("tasks", "open_tasks", self._store).current_token()

    def extract(self, token):
        self._calls += 1
        if self._calls == 1:
            self._store.create("T-clean", {"state": "ready", "summary": "cleaning staged"}, "A-ops", "plan", D(8, 59))
        return VersionedStoreSource("tasks", "open_tasks", self._store).extract(token)


class MidBuildChangeTests(unittest.TestCase):
    def test_change_during_extraction_aborts_build(self):
        tasks, equipment, incidents = _stores()
        sources = (
            FlakySource(tasks),
            VersionedStoreSource("equipment", "occupied_equipment", equipment),
            VersionedStoreSource("incidents", "site_anomalies", incidents),
        )
        service = HandoverService()
        frm, to = _parties()
        with self.assertRaises(Exception):
            service.build("H1", "MU5101", "B-2035", frm, to, sources, D(9))
        self.assertEqual(service.packages(), ())
        # The late-arriving change is real business data, but no package mixed it in.
        self.assertEqual(tasks.get("T-clean").version, 1)


class HandoverWorkflowTests(unittest.TestCase):
    def test_publish_event_only_after_release(self):
        tasks, equipment, incidents = _stores()
        sources = _sources(tasks, equipment, incidents)
        service = HandoverService()
        control = RampWorkflowControl()

        def publish():
            return _build_and_publish(service, sources)

        receipt = control.publish_ground_agent_handover("op-H1", "MU5101", D(9), publish)
        self.assertEqual(receipt.event.topic, "ground.agent.handover.published")
        self.assertEqual(control.stream.head, 1)

        # Replaying the same operation id must not rebuild or republish.
        again = control.publish_ground_agent_handover("op-H1", "MU5101", D(9), publish)
        self.assertIs(again, receipt)
        self.assertEqual(control.stream.head, 1)


if __name__ == "__main__":
    unittest.main()
