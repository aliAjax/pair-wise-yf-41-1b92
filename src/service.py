from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        kind = self.rules.normalize_kind(entity["kind"])
        if kind == "event" and action == "publish":
            communication_id = patch.get("communication_id") or (data or {}).get("communication_id")
            updated, receipt = self.repository.publish_with_receipt(
                entity_id, expected, patch, communication_id, actor.user_id
            )
            self.audit.record(
                entity_id,
                actor,
                action,
                entity["status"],
                updated["status"],
                {"patch": patch},
            )
            self.audit.record(
                receipt["id"],
                actor,
                "receipt_created",
                None,
                receipt["status"],
                {"event_id": entity_id, "communication_id": communication_id},
            )
            return updated
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        if kind == "station" and action in ("offline", "online"):
            self._invalidate_events_for_station(updated, actor)
        return updated

    def _invalidate_events_for_station(self, station_entity, actor):
        code = station_entity["data"].get("code")
        if not code:
            return
        events = self.repository.find_unpublished_events_with_station(code)
        for event in events:
            reports = event["data"].get("reports") or []
            codes = {r.get("station") for r in reports if r.get("station")}
            statuses = {}
            for station_code in codes:
                rows = self._lookup("station", "code", station_code)
                if rows:
                    statuses[station_code] = rows[0]["status"]
            updated = self.repository.invalidate_event(
                event["id"], "station %s status changed" % code, statuses
            )
            self.audit.record(
                event["id"],
                actor,
                "invalidate",
                event["status"],
                updated["status"],
                {"reason": "station status changed", "station": code},
            )

    def reconcile_receipt(self, actor, event_id, data):
        receipt = self._find_pending_receipt(event_id)
        return self.transition(actor, receipt["id"], "reconcile", data)

    def retry_receipt(self, actor, event_id):
        receipt = self._find_pending_receipt(event_id)
        return self.transition(actor, receipt["id"], "retry", {})

    def _find_pending_receipt(self, event_id):
        for receipt in self.repository.list_entities(kind="receipt"):
            if receipt["data"].get("event_id") == event_id and receipt["status"] in ("pending", "failed"):
                return receipt
        raise NotFoundError("no pending receipt for event: " + event_id)

    def receipt_queue(self):
        return [
            receipt
            for receipt in self.repository.list_entities(kind="receipt")
            if receipt["status"] in ("pending", "failed")
        ]

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
