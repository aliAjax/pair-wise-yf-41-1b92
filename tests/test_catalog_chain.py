import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import (
    Actor,
    ConflictError,
    InvalidTransition,
    ValidationError,
)
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class CatalogChainTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.repo = SQLiteRepository(Path(self.tmp.name) / "test.db")
        self.service = DomainService(self.repo, RuleEngine())
        self.admin = Actor("admin", "admin")
        self.reviewer_a = Actor("reviewer-a", "reviewer")
        self.reviewer_b = Actor("reviewer-b", "reviewer")

    def tearDown(self):
        self.tmp.cleanup()

    def _station(self, code="STA-1"):
        return self.service.create(
            self.admin, "station", {"code": code, "lat": 35.0, "lon": 110.0}
        )

    def _event(self, reports=None, **extra):
        data = {
            "title": "Event-X",
            "origin_time": "2026-01-01T00:00:00Z",
            "location": "Region-A",
            "reports": reports
            or [
                {"station": "STA-1", "time_offset": 2, "distance_km": 1.0},
                {"station": "STA-2", "time_offset": -1, "distance_km": 1.5},
            ],
        }
        data.update(extra)
        return self.service.create(self.admin, "event", data)

    def _reviewed(self, **extra):
        event = self._event(**extra)
        self.service.transition(self.admin, event["id"], "associate", {})
        return self.service.transition(
            self.reviewer_a,
            event["id"],
            "review",
            {"reviewer": "R-1", "magnitude": 4.2},
        )

    def _published(self, **extra):
        event = self._reviewed(**extra)
        return self.service.transition(
            self.reviewer_a, event["id"], "publish", {"communication_id": "C-1"}
        )

    # ---- 修订并发：先写入生效，后到冲突 ----

    def test_concurrent_revise_first_write_wins(self):
        event = self._published()
        version = event["version"]
        first = self.service.transition(
            self.reviewer_a,
            event["id"],
            "revise",
            {"reason": "r1", "magnitude": 4.5},
            expected_version=version,
        )
        self.assertEqual(first["status"], "revised")
        self.assertEqual(first["data"]["magnitude"], 4.5)
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.reviewer_b,
                event["id"],
                "revise",
                {"reason": "r2", "magnitude": 4.6},
                expected_version=version,
            )
        current = self.service.get(event["id"])
        self.assertEqual(current["data"]["magnitude"], 4.5)

    def test_concurrent_revise_with_threads(self):
        event = self._published()
        version = event["version"]
        barrier = threading.Barrier(2)
        results = {}

        def revise(actor, magnitude):
            barrier.wait()
            try:
                self.service.transition(
                    actor,
                    event["id"],
                    "revise",
                    {"reason": "race", "magnitude": magnitude},
                    expected_version=version,
                )
                results[actor.user_id] = "ok"
            except ConflictError:
                results[actor.user_id] = "conflict"

        threads = [
            threading.Thread(target=revise, args=(self.reviewer_a, 4.5)),
            threading.Thread(target=revise, args=(self.reviewer_b, 4.6)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(results.values()), ["conflict", "ok"])

    def test_revise_requires_expected_version(self):
        event = self._published()
        with self.assertRaises(ValidationError):
            self.service.transition(
                self.reviewer_a,
                event["id"],
                "revise",
                {"reason": "r", "magnitude": 4.5},
            )

    # ---- 台站状态/报告基线更新：未发布失效重算，已发布冻结 ----

    def test_station_offline_invalidates_unpublished_and_freezes_published(self):
        station = self._station("STA-1")
        reviewed = self._reviewed()
        published = self._published()
        self.service.transition(
            self.admin, station["id"], "offline", {"reason": "maintenance"}
        )
        after = self.service.get(reviewed["id"])
        self.assertEqual(after["status"], "associated")
        self.assertNotIn("magnitude", after["data"])
        self.assertTrue(after["data"]["conclusion_stale"])
        self.assertEqual(after["data"]["invalidation_cause"]["station"], "STA-1")
        frozen = self.service.get(published["id"])
        self.assertEqual(frozen["status"], "published")
        self.assertEqual(frozen["data"]["magnitude"], 4.2)
        self.assertEqual(frozen["data"]["published_snapshot"]["magnitude"], 4.2)
        audits = [
            entry
            for entry in self.service.audit_log(reviewed["id"])
            if entry["action"] == "invalidate"
        ]
        self.assertEqual(len(audits), 1)

    def test_station_back_online_invalidates_new_conclusions(self):
        station = self._station("STA-1")
        self.service.transition(
            self.admin, station["id"], "offline", {"reason": "maintenance"}
        )
        event = self._reviewed()
        self.service.transition(self.admin, station["id"], "online", {})
        after = self.service.get(event["id"])
        self.assertEqual(after["status"], "associated")
        self.assertNotIn("magnitude", after["data"])

    def test_unrelated_station_change_does_not_invalidate(self):
        self._station("STA-9")
        event = self._reviewed()
        station = self.service.list("station", status="online")[0]
        other = self._station("STA-8")
        self.service.transition(
            self.admin, other["id"], "offline", {"reason": "maintenance"}
        )
        after = self.service.get(event["id"])
        self.assertEqual(after["status"], "reviewed")
        self.assertEqual(after["data"]["magnitude"], 4.2)
        self.assertEqual(station["data"]["code"], "STA-9")

    def test_rebaseline_invalidates_unpublished_conclusion(self):
        event = self._reviewed()
        updated = self.service.transition(
            self.admin,
            event["id"],
            "rebaseline",
            {
                "reports": [
                    {"station": "STA-3", "time_offset": 1, "distance_km": 0.5},
                    {"station": "STA-4", "time_offset": 2, "distance_km": 1.0},
                ]
            },
        )
        self.assertEqual(updated["status"], "associated")
        self.assertNotIn("magnitude", updated["data"])
        self.assertTrue(updated["data"]["conclusion_stale"])
        self.assertEqual(updated["data"]["baseline_version"], 2)
        self.assertEqual(updated["data"]["associated_count"], 2)
        reviewed = self.service.transition(
            self.reviewer_a,
            event["id"],
            "review",
            {"reviewer": "R-2", "magnitude": 4.4},
        )
        self.assertEqual(reviewed["status"], "reviewed")
        self.assertFalse(reviewed["data"]["conclusion_stale"])
        self.assertEqual(reviewed["data"]["magnitude"], 4.4)

    def test_rebaseline_rejected_for_published(self):
        event = self._published()
        with self.assertRaises(InvalidTransition):
            self.service.transition(
                self.admin,
                event["id"],
                "rebaseline",
                {
                    "reports": [
                        {"station": "STA-3", "time_offset": 1, "distance_km": 0.5},
                        {"station": "STA-4", "time_offset": 2, "distance_km": 1.0},
                    ]
                },
            )

    # ---- 发布回执：失败留队列，重试沿用原通信编号，对账后完成 ----

    def test_publish_dispatch_reconcile_flow(self):
        event = self._published()
        dispatches = self.service.list("dispatch", status="pending")
        self.assertEqual(len(dispatches), 1)
        dispatch = dispatches[0]
        self.assertEqual(dispatch["data"]["event_id"], event["id"])
        self.assertEqual(dispatch["data"]["communication_id"], "C-1")
        self.assertEqual(dispatch["data"]["event_version"], event["version"])
        self.assertEqual(event["data"]["publish_receipt_status"], "pending")

        # 回执与本地记录不符：对账失败，待发单留在队列
        with self.assertRaises(ValidationError):
            self.service.transition(
                self.reviewer_a,
                dispatch["id"],
                "reconcile",
                {
                    "receipt": {
                        "communication_id": "C-1",
                        "event_id": event["id"],
                        "event_version": 999,
                    }
                },
            )
        self.assertEqual(self.service.get(dispatch["id"])["status"], "pending")
        self.assertEqual(len(self.service.list("dispatch", status="pending")), 1)

        # 重试必须沿用原通信编号
        with self.assertRaises(ValidationError):
            self.service.transition(
                self.reviewer_a,
                dispatch["id"],
                "retry",
                {"communication_id": "C-2"},
            )
        retried = self.service.transition(self.reviewer_a, dispatch["id"], "retry", {})
        self.assertEqual(retried["status"], "pending")
        self.assertEqual(retried["data"]["communication_id"], "C-1")
        self.assertEqual(retried["data"]["attempts"], 1)

        # 回执与本地记录对账一致才算完成
        done = self.service.transition(
            self.reviewer_a,
            dispatch["id"],
            "reconcile",
            {
                "receipt": {
                    "communication_id": "C-1",
                    "event_id": event["id"],
                    "event_version": event["version"],
                }
            },
        )
        self.assertEqual(done["status"], "completed")
        self.assertEqual(self.service.list("dispatch", status="pending"), [])

    def test_receipt_reconciles_against_published_version_after_revise(self):
        event = self._published()
        dispatch = self.service.list("dispatch", status="pending")[0]
        published_version = event["version"]
        self.service.transition(
            self.reviewer_a,
            event["id"],
            "revise",
            {"reason": "new data", "magnitude": 4.3},
            expected_version=published_version,
        )
        done = self.service.transition(
            self.reviewer_a,
            dispatch["id"],
            "reconcile",
            {
                "receipt": {
                    "communication_id": "C-1",
                    "event_id": event["id"],
                    "event_version": published_version,
                }
            },
        )
        self.assertEqual(done["status"], "completed")

    # ---- 旧事件按首次编目兼容 ----

    def test_legacy_event_uses_first_catalog_rules(self):
        event = self._reviewed(catalog_version=1)
        published = self.service.transition(self.reviewer_a, event["id"], "publish", {})
        self.assertEqual(published["status"], "published")
        self.assertEqual(published["data"]["publish_receipt_status"], "legacy")
        self.assertEqual(self.service.list("dispatch"), [])
        revised = self.service.transition(
            self.reviewer_a,
            event["id"],
            "revise",
            {"reason": "r", "magnitude": 4.3},
        )
        self.assertEqual(revised["status"], "revised")

    def test_legacy_event_is_not_invalidated_by_station_change(self):
        station = self._station("STA-1")
        legacy = self._reviewed(catalog_version=1)
        self.service.transition(
            self.admin, station["id"], "offline", {"reason": "maintenance"}
        )
        after = self.service.get(legacy["id"])
        self.assertEqual(after["status"], "reviewed")
        self.assertEqual(after["data"]["magnitude"], 4.2)

    def test_new_event_gets_current_catalog_version(self):
        event = self._event()
        self.assertEqual(event["data"]["catalog_version"], 2)
        self.assertEqual(event["data"]["baseline_version"], 1)


if __name__ == "__main__":
    unittest.main()
