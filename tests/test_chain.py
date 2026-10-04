import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, InvalidTransition
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class ChainTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())
        self.actor = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def _make_event(self, code1="STA-X", code2="STA-Y"):
        station1 = self.service.create(
            self.actor, "station", {"code": code1, "lat": 1.0, "lon": 2.0}
        )
        station2 = self.service.create(
            self.actor, "station", {"code": code2, "lat": 3.0, "lon": 4.0}
        )
        event = self.service.create(
            self.actor,
            "event",
            {
                "title": "Event-X",
                "origin_time": "2026-01-01T00:00:00Z",
                "location": "Region-X",
                "reports": [
                    {"station": code1, "time_offset": 2, "distance_km": 1.0},
                    {"station": code2, "time_offset": -1, "distance_km": 1.5},
                ],
            },
        )
        return station1, station2, event

    def _publish(self, event, communication_id="C-100", magnitude=4.2):
        self.service.transition(self.actor, event["id"], "associate", {})
        self.service.transition(
            self.actor,
            event["id"],
            "review",
            {"reviewer": "R-1", "magnitude": magnitude},
        )
        return self.service.transition(
            self.actor,
            event["id"],
            "publish",
            {"communication_id": communication_id},
        )

    def _receipt_for(self, event_id):
        for receipt in self.service.list(kind="receipt"):
            if receipt["data"].get("event_id") == event_id:
                return receipt
        return None

    def test_concurrent_revision_first_write_wins(self):
        _, _, event = self._make_event()
        published = self._publish(event)
        self.assertEqual(published["status"], "published")
        base_version = published["version"]

        first = self.service.transition(
            self.actor,
            event["id"],
            "revise",
            {"reason": "revision A", "magnitude": 4.3},
            expected_version=base_version,
        )
        self.assertEqual(first["status"], "revised")

        with self.assertRaises(ConflictError):
            self.service.transition(
                self.actor,
                event["id"],
                "revise",
                {"reason": "revision B", "magnitude": 4.4},
                expected_version=base_version,
            )

    def test_station_offline_invalidates_unpublished(self):
        station, _, event = self._make_event()
        self.service.transition(self.actor, event["id"], "associate", {})
        self.service.transition(
            self.actor,
            event["id"],
            "review",
            {"reviewer": "R-1", "magnitude": 4.2},
        )
        reviewed = self.service.get(event["id"])
        self.assertEqual(reviewed["status"], "reviewed")

        self.service.transition(
            self.actor,
            station["id"],
            "offline",
            {"reason": "maintenance"},
        )

        invalidated = self.service.get(event["id"])
        self.assertEqual(invalidated["status"], "candidate")
        self.assertNotIn("reviewer", invalidated["data"])
        self.assertNotIn("magnitude", invalidated["data"])
        self.assertIn("baseline", invalidated["data"])

        self.service.transition(self.actor, event["id"], "associate", {})
        self.service.transition(
            self.actor,
            event["id"],
            "review",
            {"reviewer": "R-2", "magnitude": 4.5},
        )
        recalculated = self.service.get(event["id"])
        self.assertEqual(recalculated["status"], "reviewed")
        self.assertEqual(recalculated["data"]["magnitude"], 4.5)

    def test_published_event_frozen_on_station_change(self):
        station, _, event = self._make_event()
        published = self._publish(event, communication_id="C-FROZEN")

        self.service.transition(
            self.actor,
            station["id"],
            "offline",
            {"reason": "maintenance"},
        )

        frozen = self.service.get(event["id"])
        self.assertEqual(frozen["status"], "published")
        self.assertEqual(frozen["data"]["communication_id"], "C-FROZEN")
        self.assertEqual(frozen["data"]["magnitude"], 4.2)

    def test_report_update_invalidates_unpublished(self):
        _, _, event = self._make_event()
        self.service.transition(self.actor, event["id"], "associate", {})
        self.service.transition(
            self.actor,
            event["id"],
            "review",
            {"reviewer": "R-1", "magnitude": 4.2},
        )

        new_reports = [
            {"station": "STA-X", "time_offset": 3, "distance_km": 2.0},
            {"station": "STA-Z", "time_offset": 0, "distance_km": 1.0},
        ]
        updated = self.service.transition(
            self.actor,
            event["id"],
            "update_reports",
            {"reports": new_reports},
        )
        self.assertEqual(updated["status"], "candidate")
        self.assertEqual(updated["data"]["reports"], new_reports)
        self.assertIsNone(updated["data"].get("reviewer"))
        self.assertIsNone(updated["data"].get("magnitude"))

        self.service.transition(self.actor, event["id"], "associate", {})
        self.assertEqual(self.service.get(event["id"])["status"], "associated")

    def test_published_event_rejects_report_update(self):
        _, _, event = self._make_event()
        self._publish(event)
        with self.assertRaises(InvalidTransition):
            self.service.transition(
                self.actor,
                event["id"],
                "update_reports",
                {
                    "reports": [
                        {"station": "STA-X", "time_offset": 3, "distance_km": 2.0},
                        {"station": "STA-Z", "time_offset": 0, "distance_km": 1.0},
                    ]
                },
            )

    def test_receipt_lifecycle(self):
        _, _, event = self._make_event()
        self._publish(event, communication_id="C-100", magnitude=4.2)

        receipt = self._receipt_for(event["id"])
        self.assertIsNotNone(receipt)
        self.assertEqual(receipt["status"], "pending")
        self.assertEqual(receipt["data"]["communication_id"], "C-100")
        self.assertEqual(receipt["data"]["attempts"], 0)

        retried = self.service.transition(self.actor, receipt["id"], "retry", {})
        self.assertEqual(retried["status"], "pending")
        self.assertEqual(retried["data"]["communication_id"], "C-100")
        self.assertEqual(retried["data"]["attempts"], 1)

        with self.assertRaises(ConflictError):
            self.service.transition(
                self.actor,
                receipt["id"],
                "reconcile",
                {"communication_id": "C-100", "magnitude": 9.9},
            )
        self.assertEqual(self.service.get(receipt["id"])["status"], "pending")

        failed = self.service.transition(
            self.actor,
            receipt["id"],
            "fail",
            {"error": "network down"},
        )
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["data"]["last_error"], "network down")

        retried_again = self.service.transition(self.actor, receipt["id"], "retry", {})
        self.assertEqual(retried_again["status"], "pending")
        self.assertEqual(retried_again["data"]["communication_id"], "C-100")
        self.assertEqual(retried_again["data"]["attempts"], 2)

        reconciled = self.service.transition(
            self.actor,
            receipt["id"],
            "reconcile",
            {"communication_id": "C-100", "magnitude": 4.2},
        )
        self.assertEqual(reconciled["status"], "reconciled")
        self.assertNotIn(reconciled, self.service.receipt_queue())

    def test_reconcile_by_communication_keeps_number(self):
        _, _, event = self._make_event()
        self._publish(event, communication_id="C-200", magnitude=4.2)

        receipt = self._receipt_for(event["id"])
        self.service.transition(self.actor, receipt["id"], "fail", {"error": "timeout"})
        self.service.retry_receipt(self.actor, event["id"])

        retried = self._receipt_for(event["id"])
        self.assertEqual(retried["status"], "pending")
        self.assertEqual(retried["data"]["communication_id"], "C-200")

        reconciled = self.service.reconcile_receipt(
            self.actor,
            event["id"],
            {"communication_id": "C-200", "magnitude": 4.2},
        )
        self.assertEqual(reconciled["status"], "reconciled")

    def test_old_event_gets_baseline_on_first_cataloging(self):
        station, _, event = self._make_event()
        self.assertNotIn("baseline", event["data"])

        associated = self.service.transition(self.actor, event["id"], "associate", {})
        self.assertIn("baseline", associated["data"])
        self.assertEqual(
            associated["data"]["baseline"]["station_statuses"].get(station["data"]["code"]),
            "online",
        )


if __name__ == "__main__":
    unittest.main()
