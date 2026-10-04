from datetime import datetime, timedelta

from .domain import (
    ConflictError,
    InvalidTransition,
    PermissionDenied,
    ValidationError,
)

CURRENT_CATALOG_VERSION = 2
CONCLUSION_FIELDS = ("magnitude", "reviewer")


def catalog_version_of(entity):
    """首次编目时写入的规则版本，缺失按旧版(1)处理。"""
    data = entity.get("data") or {}
    try:
        return int(data.get("catalog_version") or 1)
    except (TypeError, ValueError):
        return 1


def is_legacy_event(entity):
    return entity.get("kind") == "event" and catalog_version_of(entity) < CURRENT_CATALOG_VERSION


def event_references_station(event, station_code):
    reports = (event.get("data") or {}).get("reports") or []
    return any(str(report.get("station")) == str(station_code) for report in reports)


def invalidate_conclusion(cause, now):
    """未发布结论失效：返回(合并补丁, 需删除的结论字段)。"""
    patch = {
        "conclusion_stale": True,
        "invalidation_cause": cause,
        "invalidated_at": now,
    }
    return patch, list(CONCLUSION_FIELDS)


def _validate_station(actor, data, lookup):
    if not data.get("code"):
        raise ValidationError("station code is required")


def _validate_event(actor, data, lookup):
    reports = data.get("reports") or []
    if len(reports) < 2:
        raise ValidationError("event requires at least two station reports")
    if not data.get("title"):
        raise ValidationError("event title is required")
    data.setdefault("catalog_version", CURRENT_CATALOG_VERSION)
    data.setdefault("baseline_version", 1)


def _validate_associate(actor, entity, data, lookup):
    reports = entity["data"].get("reports") or []
    if len(reports) < 2:
        raise ValidationError("two reports are required for association")
    return {"associated_count": len(reports)}


def _validate_review(actor, entity, data, lookup):
    return {
        "conclusion_stale": False,
        "reviewed_baseline_version": entity["data"].get("baseline_version") or 1,
    }


def _validate_publish(actor, entity, data, lookup):
    if is_legacy_event(entity):
        return {"publish_receipt_status": "legacy"}
    if not data.get("communication_id"):
        raise ValidationError("missing required field: communication_id")
    snapshot = {
        "magnitude": entity["data"].get("magnitude"),
        "reviewer": entity["data"].get("reviewer"),
        "reports": entity["data"].get("reports") or [],
        "baseline_version": entity["data"].get("baseline_version") or 1,
        "communication_id": data.get("communication_id"),
        "catalog_version": catalog_version_of(entity),
    }
    return {"publish_receipt_status": "pending", "published_snapshot": snapshot}


def _validate_rebaseline(actor, entity, data, lookup):
    reports = data.get("reports") or []
    if len(reports) < 2:
        raise ValidationError("event requires at least two station reports")
    associated = associate_reports(reports)
    baseline_version = int(entity["data"].get("baseline_version") or 1) + 1
    return {"associated_count": len(associated), "baseline_version": baseline_version}


def _validate_retry(actor, entity, data, lookup):
    original = entity["data"].get("communication_id")
    requested = data.get("communication_id")
    if requested is not None and str(requested) != str(original):
        raise ValidationError("retry must reuse the original communication_id")
    attempts = int(entity["data"].get("attempts") or 0) + 1
    return {"attempts": attempts, "communication_id": original}


def _validate_reconcile(actor, entity, data, lookup):
    receipt = data.get("receipt") or {}
    record = entity["data"]
    mismatched = [
        field
        for field in ("communication_id", "event_id", "event_version")
        if str(receipt.get(field)) != str(record.get(field))
    ]
    if mismatched:
        raise ValidationError("receipt does not match local record: " + ", ".join(mismatched))
    return {"receipt": receipt}


def associate_reports(reports, max_delta=120, max_distance=3.0):
    if not reports:
        return []
    anchor = reports[0]
    result = [anchor]
    for report in reports[1:]:
        if abs(float(report.get("time_offset", 0))) <= max_delta and float(report.get("distance_km", 0)) <= max_distance:
            result.append(report)
    return result


def magnitude_median(amplitudes):
    values = sorted(float(value) for value in amplitudes)
    if not values:
        raise ValidationError("amplitudes are required")
    middle = len(values) // 2
    if len(values) % 2:
        return values[middle]
    return (values[middle - 1] + values[middle]) / 2.0


CUSTOM_CREATE = {'station': _validate_station, 'event': _validate_event}
CUSTOM_TRANSITIONS = {
    ('event', 'associate'): _validate_associate,
    ('event', 'review'): _validate_review,
    ('event', 'publish'): _validate_publish,
    ('event', 'rebaseline'): _validate_rebaseline,
    ('dispatch', 'retry'): _validate_retry,
    ('dispatch', 'reconcile'): _validate_reconcile,
}


class RuleEngine:
    ALIASES = {'stations': 'station', 'events': 'event', 'dispatches': 'dispatch'}
    INITIAL_STATUS = {'station': 'online', 'event': 'candidate', 'dispatch': 'pending'}
    TRANSITIONS = {
        'station': {
            'offline': (('online',), 'offline'),
            'online': (('offline',), 'online'),
        },
        'event': {
            'associate': (('candidate',), 'associated'),
            'review': (('associated',), 'reviewed'),
            'publish': (('reviewed',), 'published'),
            'revise': (('published', 'revised'), 'revised'),
            'withdraw': (('published', 'revised'), 'withdrawn'),
            'rebaseline': (('associated', 'reviewed'), 'associated'),
        },
        'dispatch': {
            'retry': (('pending',), 'pending'),
            'reconcile': (('pending',), 'completed'),
        },
    }
    CREATE_REQUIRED = {
        'station': ('code', 'lat', 'lon'),
        'event': ('title', 'origin_time', 'location', 'reports'),
        'dispatch': ('event_id', 'communication_id', 'event_version'),
    }
    ACTION_REQUIRED = {
        ('station', 'offline'): ('reason',),
        ('event', 'review'): ('reviewer', 'magnitude'),
        ('event', 'revise'): ('reason', 'magnitude'),
        ('event', 'withdraw'): ('reason',),
        ('event', 'rebaseline'): ('reports',),
        ('dispatch', 'reconcile'): ('receipt',),
    }
    CREATE_ROLES = {'station': ('admin', 'station'), 'event': ('admin', 'analyst'), 'dispatch': ('admin', 'reviewer')}
    ROLE_ACTIONS = {
        'offline': ('admin', 'station'),
        'online': ('admin', 'station'),
        'associate': ('admin', 'analyst'),
        'review': ('admin', 'reviewer'),
        'publish': ('admin', 'reviewer'),
        'revise': ('admin', 'reviewer'),
        'withdraw': ('admin', 'reviewer'),
        'rebaseline': ('admin', 'analyst'),
        'retry': ('admin', 'reviewer'),
        'reconcile': ('admin', 'reviewer'),
    }
    STRICT_EXPECTED_VERSION = {('event', 'revise')}

    def normalize_kind(self, kind):
        return self.ALIASES.get(kind, kind)

    def initial_status(self, kind):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        return self.INITIAL_STATUS[kind]

    def expected_version_required(self, kind, action, entity):
        kind = self.normalize_kind(kind)
        if (kind, action) not in self.STRICT_EXPECTED_VERSION:
            return False
        return not is_legacy_event(entity)

    @staticmethod
    def _ensure_role(actor, allowed):
        if "*" not in allowed and actor.role not in allowed:
            raise PermissionDenied("role %s is not allowed here" % actor.role)

    @staticmethod
    def _require(data, fields):
        for field in fields:
            value = data.get(field)
            if value is None or value == "" or value == [] or value == {}:
                raise ValidationError("missing required field: " + field)

    def validate_create(self, actor, kind, data, lookup=None):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        self._ensure_role(actor, self.CREATE_ROLES.get(kind, ("admin",)))
        self._require(data, self.CREATE_REQUIRED.get(kind, ()))
        custom = CUSTOM_CREATE.get(kind)
        if custom:
            custom(actor, data, lookup)
        return dict(data)

    def validate_transition(self, actor, entity, action, data, lookup=None):
        kind = self.normalize_kind(entity["kind"])
        transition = self.TRANSITIONS.get(kind, {}).get(action)
        if not transition:
            raise InvalidTransition("unknown action %s for %s" % (action, kind))
        allowed_statuses, next_status = transition
        if entity["status"] not in allowed_statuses:
            raise InvalidTransition(
                "cannot %s from status %s" % (action, entity["status"])
            )
        allowed_roles = self.ROLE_ACTIONS.get(
            (kind, action), self.ROLE_ACTIONS.get(action, ("admin",))
        )
        self._ensure_role(actor, allowed_roles)
        self._require(data, self.ACTION_REQUIRED.get((kind, action), ()))
        custom = CUSTOM_TRANSITIONS.get((kind, action))
        extra = custom(actor, entity, data, lookup) if custom else {}
        patch = dict(data)
        if extra:
            patch.update(extra)
        return next_status, patch


def _find_one(lookup, kind, field, value):
    if lookup is None:
        return None
    rows = lookup(kind, field, value) or []
    return rows[0] if rows else None


def _date_ordinal(value):
    return datetime.fromisoformat(str(value)[:10]).date().toordinal()
