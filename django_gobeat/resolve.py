"""Turn a configured Celery app into the JSON document gobeat reads.

Everything that needs project code (task registry, ``task_routes``,
decorator options, queue declarations) is resolved here, by Celery itself,
so that the Go side never has to guess.
"""
import contextlib
import datetime
import hashlib
import json
import numbers
import platform

from . import SETTINGS_SCHEMA

#: Publish options whose values gobeat understands.
EXEC_OPTIONS = (
    'queue', 'routing_key', 'exchange', 'priority', 'expires', 'serializer',
    'delivery_mode', 'compression', 'time_limit', 'soft_time_limit',
    'immediate', 'mandatory',
)
ROUTE_KEYS = frozenset((
    'queue', 'exchange', 'routing_key', 'exchange_type', 'priority',
    'delivery_mode', 'serializer', 'compression',
))


class Unsupported(Exception):
    """Something gobeat cannot reproduce; the task is refused, not guessed."""


def _plain(value, what):
    """Return ``value`` if it is JSON-representable without loss."""
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        return float(value)
    if isinstance(value, (list, tuple)):
        return [_plain(v, what) for v in value]
    if isinstance(value, dict):
        return {str(k): _plain(v, what) for k, v in value.items()}
    raise Unsupported(f'{what}: unsupported value {value!r}')


def _exchange_name(exchange):
    return getattr(exchange, 'name', exchange)


def queue_to_dict(queue):
    """Describe a ``kombu.Queue`` the way ``Queue.declare`` would use it."""
    from kombu.transport.pyamqp import to_rabbitmq_queue_arguments

    ex = queue.exchange
    if getattr(queue, 'bindings', None):
        raise Unsupported(f'queue {queue.name!r} uses multiple bindings')
    return {
        'name': queue.name,
        'routing_key': queue.routing_key,
        'durable': bool(queue.durable),
        'exclusive': bool(queue.exclusive),
        'auto_delete': bool(queue.auto_delete),
        'no_declare': bool(queue.no_declare),
        # kombu/entity.py: Queue.queue_declare ->
        # kombu/transport/pyamqp.py: Channel.prepare_queue_arguments
        'queue_arguments': _plain(to_rabbitmq_queue_arguments(
            queue.queue_arguments or {},
            expires=queue.expires,
            message_ttl=queue.message_ttl,
            max_length=queue.max_length,
            max_length_bytes=queue.max_length_bytes,
            max_priority=queue.max_priority,
        ), f'queue {queue.name!r} arguments'),
        'binding_arguments': _plain(queue.binding_arguments,
                                    f'queue {queue.name!r} binding'),
        'exchange': {
            'name': ex.name if ex is not None else '',
            'type': ex.type if ex is not None else 'direct',
            'durable': bool(ex.durable) if ex is not None else True,
            'auto_delete': bool(ex.auto_delete) if ex is not None else False,
            'delivery_mode': (int(ex.delivery_mode)
                              if ex is not None and ex.delivery_mode else None),
            'arguments': _plain(ex.arguments if ex is not None else None,
                                f'queue {queue.name!r} exchange'),
            'no_declare': bool(ex.no_declare) if ex is not None else True,
            'passive': bool(ex.passive) if ex is not None else False,
        },
    }


class _Captured(Exception):
    pass


class _CapturingProducer:
    """Stands in for ``kombu.Producer``; records the publish call."""

    class _Conn:
        @contextlib.contextmanager
        def _reraise_as_library_errors(self, *a, **k):
            yield

    def __init__(self):
        self.connection = self._Conn()
        self.call = None

    def publish(self, body, **kwargs):
        self.call = (body, kwargs)


def capture_publish(app, task, name, args=None, kwargs=None, **options):
    """Run Celery's real send path and return the ``Producer.publish``
    keyword arguments without touching the broker or result backend.

    This is ``Scheduler.apply_async`` from celery/beat.py: a registered
    task goes through ``task.apply_async``, anything else through
    ``app.send_task``.
    """
    from unittest import mock

    producer = _CapturingProducer()
    backend_cls = type(app.backend)
    with mock.patch.object(backend_cls, 'on_task_call',
                           lambda self, *a, **k: None):
        if task is not None:
            typing = task.typing
            task.typing = False       # we have no real arguments to check
            try:
                task.apply_async(args, kwargs, producer=producer, **options)
            finally:
                task.typing = typing
        else:
            app.send_task(name, args, kwargs, producer=producer, **options)
    body, call = producer.call
    declare = call.get('declare') or []
    exchange = call.get('exchange')
    return {
        'exchange': _exchange_name(exchange) or '',
        'routing_key': call.get('routing_key'),
        'declare': [q.name for q in declare],
        'serializer': call.get('serializer'),
        'compression': call.get('compression'),
        'delivery_mode': call.get('delivery_mode'),
        'has_priority': 'priority' in call,
        'priority': call.get('priority'),
        'expiration': call.get('expiration'),
        'ignore_result': call['headers'].get('ignore_result'),
        'has_expires': call['headers'].get('expires') is not None,
        'timelimit': call['headers'].get('timelimit'),
    }


def _route_for(app, router, name, options, task, known_queues):
    """Raw result of ``Router.lookup_route`` (celery/app/routes.py)."""
    if not router.routes:
        return None
    route = router.lookup_route(name, (), {}, dict(options), task)
    if not route:
        return None
    if isinstance(route, str):
        route = {'queue': route}
    route = dict(route)
    unknown = set(route) - ROUTE_KEYS
    if unknown:
        raise Unsupported(
            f'route for {name!r} uses unsupported keys {sorted(unknown)}')
    return _named_queue(route, name, known_queues)


def _named_queue(options, name, known_queues):
    """Replace a ``kombu.Queue`` value by its name when it is the queue
    registered under that name; refuse anonymous Queue objects."""
    from kombu import Exchange, Queue

    out = {}
    for key, value in options.items():
        if isinstance(value, Queue):
            if known_queues.get(value.name) is not value:
                raise Unsupported(
                    f'{name!r}: {key} is a kombu.Queue that is not in '
                    f'task_queues')
            value = value.name
        elif isinstance(value, Exchange):
            value = value.name
        elif isinstance(value, (datetime.datetime, datetime.timedelta)):
            raise Unsupported(f'{name!r}: {key}={value!r} is not a number')
        out[key] = _plain(value, f'{name!r} option {key}')
    return out


def build_settings(app, extra_task_names=(), include_broker_url=True):
    """Return the settings document for ``app``."""
    import celery
    import django
    import django_celery_beat
    import kombu
    from celery.app.routes import MapRoute
    from django.conf import settings as dj_settings
    from kombu.utils.functional import maybe_evaluate

    conf = app.conf
    warnings = []
    app.finalize(auto=True)
    queues_obj = app.amqp.queues
    router = app.amqp.router

    tz_name = conf.timezone
    if tz_name is not None and not isinstance(tz_name, str):
        tz_name = getattr(tz_name, 'key', None) or str(tz_name)

    dynamic_routes = any(
        not isinstance(maybe_evaluate(r), MapRoute) for r in router.routes)
    if dynamic_routes:
        warnings.append(
            'task_routes contains router functions/classes; they were '
            'evaluated once per task with empty args/kwargs. Routes that '
            'depend on call arguments are not reproduced by gobeat.')
    if conf.task_always_eager:
        warnings.append('task_always_eager is set: Python beat would run '
                        'tasks inline; gobeat always publishes.')
    if conf.task_send_sent_event:
        warnings.append('task_send_sent_event is set: gobeat does not emit '
                        'task-sent events.')
    if conf.task_protocol != 2:
        warnings.append(f'task_protocol={conf.task_protocol}: gobeat only '
                        'speaks protocol 2.')

    tasks = {}
    names = list(app.tasks.keys())
    names += [n for n in extra_task_names if n not in app.tasks]
    for name in sorted(set(names)):
        task = app.tasks.get(name)
        entry = {'registered': task is not None}
        try:
            if task is not None:
                options = _named_queue(dict(task._get_exec_options()), name,
                                       queues_obj)
                entry['options'] = {k: options.get(k) for k in EXEC_OPTIONS}
                entry['ignore_result'] = bool(task.ignore_result)
            else:
                entry['options'] = {}
                entry['ignore_result'] = False
            entry['route'] = _route_for(app, router, name, entry['options'],
                                        task, queues_obj)
        except Unsupported as exc:
            entry['unsupported'] = str(exc)
            warnings.append(str(exc))
        except Exception as exc:  # noqa: BLE001 - e.g. a failing router
            entry['unsupported'] = (
                f'{name!r}: could not resolve options/route: '
                f'{type(exc).__name__}: {exc}')
            warnings.append(entry['unsupported'])
        else:
            # What Celery itself does for this task without PeriodicTask
            # overrides; gobeat recomputes it and refuses the task if the
            # two disagree. A failure here (for example a decorator queue
            # missing from task_queues) is what Python beat would hit too,
            # unless a PeriodicTask overrides the offending option.
            try:
                entry['resolved'] = capture_publish(app, task, name)
            except Exception as exc:  # noqa: BLE001 - record, never guess
                entry['resolved'] = {
                    'error': f'{type(exc).__name__}: {exc}'}
                warnings.append(
                    f'{name!r}: Celery cannot publish this task with its '
                    f'default options: {type(exc).__name__}: {exc}')
        tasks[name] = entry

    # Queues are collected last: resolving tasks may auto-create missing
    # queues (celery/app/amqp.py: Queues.__missing__).
    queues = {}
    for qname, queue in queues_obj.items():
        try:
            queues[qname] = queue_to_dict(queue)
        except Unsupported as exc:
            queues[qname] = {'name': qname, 'unsupported': str(exc)}
            warnings.append(str(exc))

    default_exchange = app.amqp.default_exchange
    deadline = conf.beat_cron_starting_deadline
    data = {
        'schema': SETTINGS_SCHEMA,
        'versions': {
            'celery': celery.__version__,
            'kombu': kombu.__version__,
            'django_celery_beat': django_celery_beat.__version__,
            'django': django.get_version(),
            'python': platform.python_version(),
        },
        'broker_url': ((conf.broker_write_url or conf.broker_url)
                       if include_broker_url else None),
        'broker_transport_options': _plain(
            dict(conf.broker_transport_options or {}),
            'broker_transport_options'),
        'broker_use_ssl': bool(conf.broker_use_ssl),
        'timezone': tz_name,
        'enable_utc': bool(conf.enable_utc),
        'use_tz': bool(getattr(dj_settings, 'USE_TZ', False)),
        'tz_aware': bool(getattr(dj_settings, 'DJANGO_CELERY_BEAT_TZ_AWARE',
                                 True)),
        # django_celery_beat/schedulers.py: DatabaseScheduler.__init__
        'beat_max_loop_interval': conf.beat_max_loop_interval or 5,
        'beat_cron_starting_deadline': (
            deadline.total_seconds()
            if isinstance(deadline, datetime.timedelta) else deadline),
        'task_default_queue': conf.task_default_queue,
        'task_default_exchange': _exchange_name(default_exchange),
        'task_default_exchange_type': conf.task_default_exchange_type,
        'task_default_routing_key': conf.task_default_routing_key,
        'task_default_delivery_mode': conf.task_default_delivery_mode,
        'task_default_priority': conf.task_default_priority,
        'task_serializer': conf.task_serializer,
        'task_compression': conf.task_compression,
        'task_create_missing_queues': bool(conf.task_create_missing_queues),
        'task_queue_max_priority': conf.task_queue_max_priority,
        'task_protocol': conf.task_protocol,
        'task_repr_maxlevels': conf.get('task_repr_maxlevels', 3),
        'task_always_eager': bool(conf.task_always_eager),
        'task_send_sent_event': bool(conf.task_send_sent_event),
        'has_routes': bool(router.routes),
        'dynamic_routes': dynamic_routes,
        'queues': queues,
        'tasks': tasks,
        'warnings': warnings,
    }
    return data


def canonical_json(data):
    return json.dumps(data, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True)


def version_hash(data):
    return hashlib.sha256(canonical_json(data).encode('ascii')).hexdigest()
