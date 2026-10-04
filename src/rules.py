from datetime import datetime, timedelta, timezone

from .domain import (
    ConflictError,
    InvalidTransition,
    NotFoundError,
    PermissionDenied,
    ValidationError,
)


FROZEN_STATUSES = ("published", "revised")
UNPUBLISHED_STATUSES = ("candidate", "associated", "reviewed")


def _utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _validate_station(actor, data, lookup):
    if not data.get("code"):
        raise ValidationError("station code is required")


def _validate_event(actor, data, lookup):
    reports = data.get("reports") or []
    if len(reports) < 2:
        raise ValidationError("event requires at least two station reports")
    if not data.get("title"):
        raise ValidationError("event title is required")


def _build_baseline(reports, lookup):
    codes = {
        report.get("station")
        for report in reports
        if report.get("station")
    }
    station_statuses = {}
    for code in codes:
        rows = lookup("station", "code", code) if lookup else None
        if rows:
            station_statuses[code] = rows[0].get("status")
    return {"reports": list(reports), "station_statuses": station_statuses}


def _validate_associate(actor, entity, data, lookup):
    reports = entity["data"].get("reports") or []
    if len(reports) < 2:
        raise ValidationError("two reports are required for association")
    return {
        "associated_count": len(reports),
        "baseline": _build_baseline(reports, lookup),
    }


def _validate_review(actor, entity, data, lookup):
    reports = entity["data"].get("reports") or []
    return {"baseline": _build_baseline(reports, lookup)}


def _validate_publish(actor, entity, data, lookup):
    reports = entity["data"].get("reports") or []
    return {"baseline": _build_baseline(reports, lookup)}


def _validate_update_reports(actor, entity, data, lookup):
    reports = data.get("reports") or []
    if len(reports) < 2:
        raise ValidationError("event requires at least two station reports")
    return {
        "reports": reports,
        "associated_count": None,
        "reviewer": None,
        "magnitude": None,
        "baseline": _build_baseline(reports, lookup),
        "invalidated_at": _utcnow(),
        "invalidation_reason": "report baseline updated",
    }


def _validate_reconcile(actor, entity, data, lookup):
    stored = entity.get("data", {})
    event_id = stored.get("event_id")
    rows = lookup("event", "id", event_id) if lookup else None
    event = rows[0] if rows else None
    if not event:
        raise NotFoundError("event not found for receipt: " + str(event_id))
    if event.get("status") not in FROZEN_STATUSES:
        raise ConflictError(
            "event %s is not published; receipt cannot reconcile" % event_id
        )
    if str(data.get("communication_id")) != str(stored.get("communication_id")):
        raise ConflictError(
            "communication_id mismatch: receipt %s, local %s"
            % (data.get("communication_id"), stored.get("communication_id"))
        )
    if float(data.get("magnitude")) != float(stored.get("magnitude")):
        raise ConflictError(
            "magnitude mismatch: receipt %s, local %s"
            % (data.get("magnitude"), stored.get("magnitude"))
        )
    if float(stored.get("magnitude")) != float(event["data"].get("magnitude")):
        raise ConflictError("receipt does not match local event record")
    return {
        "reconciled_at": _utcnow(),
        "reconciled_by": actor.user_id,
        "last_error": None,
    }


def _validate_retry(actor, entity, data, lookup):
    stored = entity.get("data", {})
    attempts = int(stored.get("attempts", 0)) + 1
    return {
        "attempts": attempts,
        "last_error": None,
        "retried_at": _utcnow(),
    }


def _validate_fail(actor, entity, data, lookup):
    return {
        "last_error": data.get("error") or data.get("reason") or "delivery failed",
        "failed_at": _utcnow(),
    }


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
    ('event', 'update_reports'): _validate_update_reports,
    ('receipt', 'reconcile'): _validate_reconcile,
    ('receipt', 'retry'): _validate_retry,
    ('receipt', 'fail'): _validate_fail,
}


class RuleEngine:
    ALIASES = {'stations': 'station', 'events': 'event', 'receipts': 'receipt'}
    INITIAL_STATUS = {'station': 'online', 'event': 'candidate', 'receipt': 'pending'}
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
            'update_reports': (UNPUBLISHED_STATUSES, 'candidate'),
        },
        'receipt': {
            'reconcile': (('pending', 'failed'), 'reconciled'),
            'fail': (('pending',), 'failed'),
            'retry': (('failed', 'pending'), 'pending'),
        },
    }
    CREATE_REQUIRED = {
        'station': ('code', 'lat', 'lon'),
        'event': ('title', 'origin_time', 'location', 'reports'),
        'receipt': ('event_id', 'communication_id'),
    }
    ACTION_REQUIRED = {
        ('station', 'offline'): ('reason',),
        ('event', 'review'): ('reviewer', 'magnitude'),
        ('event', 'publish'): ('communication_id',),
        ('event', 'revise'): ('reason', 'magnitude'),
        ('event', 'withdraw'): ('reason',),
        ('event', 'update_reports'): ('reports',),
        ('receipt', 'reconcile'): ('communication_id', 'magnitude'),
        ('receipt', 'fail'): ('error',),
        ('receipt', 'retry'): (),
    }
    CREATE_ROLES = {
        'station': ('admin', 'station'),
        'event': ('admin', 'analyst'),
        'receipt': ('admin', 'reviewer'),
    }
    ROLE_ACTIONS = {
        'offline': ('admin', 'station'),
        'online': ('admin', 'station'),
        'associate': ('admin', 'analyst'),
        'review': ('admin', 'reviewer'),
        'publish': ('admin', 'reviewer'),
        'revise': ('admin', 'reviewer'),
        'withdraw': ('admin', 'reviewer'),
        'update_reports': ('admin', 'analyst'),
        'reconcile': ('admin', 'reviewer'),
        'fail': ('admin', 'reviewer'),
        'retry': ('admin', 'reviewer'),
    }

    def normalize_kind(self, kind):
        return self.ALIASES.get(kind, kind)

    def initial_status(self, kind):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        return self.INITIAL_STATUS[kind]

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
