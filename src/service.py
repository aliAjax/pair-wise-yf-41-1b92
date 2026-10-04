from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, ValidationError
from .repository import utcnow
from .rules import (
    RuleEngine,
    event_references_station,
    invalidate_conclusion,
    is_legacy_event,
)


class DomainService:
    STATION_CASCADE_ACTIONS = ("offline", "online")

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
        kind = self.rules.normalize_kind(entity["kind"])
        if expected_version is None and self.rules.expected_version_required(kind, action, entity):
            raise ValidationError("expected_version is required for action: " + action)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        if kind == "event" and action == "rebaseline" and entity["status"] == "reviewed":
            self._apply_invalidation(merged, {"type": "rebaseline"})
        needs_dispatch = (
            kind == "event" and action == "publish" and not is_legacy_event(entity)
        )
        cascade = []
        if kind == "station" and action in self.STATION_CASCADE_ACTIONS:
            cascade = self._plan_station_cascade(entity, next_status, action)
        with self.repository.transaction() as conn:
            updated = self.repository.update_entity(
                entity_id, expected, next_status, merged, conn=conn
            )
            self.audit.record(
                entity_id,
                actor,
                action,
                entity["status"],
                updated["status"],
                {"patch": patch},
                conn=conn,
            )
            if needs_dispatch:
                self._create_dispatch(actor, updated, patch, conn)
            for target in cascade:
                self.repository.update_entity(
                    target["id"], target["version"], "associated", target["data"], conn=conn
                )
                self.audit.record(
                    target["id"],
                    actor,
                    "invalidate",
                    "reviewed",
                    "associated",
                    {"cause": target["cause"]},
                    conn=conn,
                )
        return updated

    @staticmethod
    def _apply_invalidation(merged, cause):
        patch, drop = invalidate_conclusion(cause, utcnow())
        merged.update(patch)
        for key in drop:
            merged.pop(key, None)

    def _create_dispatch(self, actor, event, patch, conn):
        dispatch_id = "disp-" + uuid4().hex
        data = {
            "event_id": event["id"],
            "communication_id": patch.get("communication_id"),
            "event_version": event["version"],
            "attempts": 0,
        }
        dispatch = self.repository.create_entity(
            dispatch_id,
            "dispatch",
            self.rules.initial_status("dispatch"),
            data,
            actor.user_id,
            conn=conn,
        )
        self.audit.record(
            dispatch["id"],
            actor,
            "create",
            None,
            dispatch["status"],
            {"kind": "dispatch", "event_id": event["id"]},
            conn=conn,
        )
        return dispatch

    def _plan_station_cascade(self, station, next_status, action):
        code = station["data"].get("code")
        if not code:
            return []
        planned = []
        for event in self.repository.list_entities(kind="event", status="reviewed"):
            if is_legacy_event(event) or not event_references_station(event, code):
                continue
            cause = {
                "type": "station_status",
                "station": code,
                "station_status": next_status,
                "trigger_action": action,
            }
            merged = dict(event["data"])
            self._apply_invalidation(merged, cause)
            planned.append(
                {
                    "id": event["id"],
                    "version": event["version"],
                    "data": merged,
                    "cause": cause,
                }
            )
        return planned

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
