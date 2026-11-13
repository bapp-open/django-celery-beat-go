"""Message golden files: what real Celery/kombu put on the wire.

For every case a ``PeriodicTask`` row is turned into a ``ModelEntry`` and
sent through ``DatabaseScheduler.apply_async`` (celery/beat.py) - the same
call Python beat makes - once against a real Redis and once against a real
RabbitMQ. Only non-deterministic inputs are pinned: the task id, kombu's
delivery tag, the reply-to id, the origin node name and ``app.now``.

Needs GOLDEN_REDIS_URL and GOLDEN_AMQP_URL (see ``make golden``).
"""
import base64
import json
import os
import random
import sys
from datetime import datetime, timedelta, timezone as dt_timezone

UTC = dt_timezone.utc

REDIS_URL = os.environ.get('GOLDEN_REDIS_URL')
AMQP_URL = os.environ.get('GOLDEN_AMQP_URL')

ORIGIN = 'gen1@golden'
REPLY_TO = '11111111-2222-3333-4444-555555555555'


# --------------------------------------------------------------------------
# random JSON text, the way it can sit in PeriodicTask.args/kwargs/headers
# --------------------------------------------------------------------------
WORDS = ['a', 'b', 'key', 'user_id', 'x y', "it's", 'q"uote', 'é', 'ключ',
         '日本', '😀', 'tab\there', 'nl\nhere', 'back\\slash', '', ' ',
         'None', 'True', '\x7f', ' ', ' ', 'a' * 40, '\x01']


def rand_string(rng):
    r = rng.random()
    if r < 0.6:
        return rng.choice(WORDS)
    if r < 0.8:
        return ''.join(rng.choice('abc xyz_-.,:;\'"/\\') for _ in
                       range(rng.randint(0, 20)))
    if r < 0.9:
        return ''.join(chr(rng.choice([rng.randint(0x20, 0x7e),
                                       rng.randint(0xa0, 0x2ff),
                                       rng.randint(0x400, 0x4ff),
                                       rng.randint(0x4e00, 0x4eff),
                                       rng.randint(0x1f600, 0x1f64f),
                                       rng.randint(0, 0x1f)]))
                       for _ in range(rng.randint(1, 12)))
    return 'L' * rng.randint(100, 1500)


def rand_number_text(rng):
    r = rng.random()
    if r < 0.4:
        return str(rng.randint(-1000, 1000))
    if r < 0.5:
        return str(rng.choice([2 ** 31, 2 ** 53 + 1, 2 ** 64, -2 ** 63,
                               10 ** 30, 0, -0]))
    if r < 0.6:
        return rng.choice(['-0', '0.0', '-0.0', '1.0', '1.50', '1E5', '1e5',
                           '1e-7', '1.0e+16', '1e16', '1e22', '1e23',
                           '123456789012345678.0', '0.1', '0.30000000000000004',
                           '5e-324', '1.7976931348623157e308', '1e400',
                           '-1e400', '2.5E-5', '100.0', '1e15', '9.999e15',
                           '0.0001', '0.00001'])
    if r < 0.63:
        return rng.choice(['NaN', 'Infinity', '-Infinity'])
    return repr(rng.uniform(-1e6, 1e6) * 10 ** rng.randint(-12, 12))


def enc_string(rng, s):
    if rng.random() < 0.5:
        return json.dumps(s)
    return json.dumps(s, ensure_ascii=False)


def rand_json_text(rng, depth=0, container=None):
    """Return JSON *text* with varied formatting."""
    sp = lambda: rng.choice(['', '', ' ', '  ', '\n ', '\t'])  # noqa: E731
    kind = container or rng.choice(
        ['num', 'num', 'str', 'str', 'bool', 'null', 'list', 'dict']
        if depth < 4 else ['num', 'str', 'bool', 'null'])
    if kind == 'num':
        return rand_number_text(rng)
    if kind == 'str':
        return enc_string(rng, rand_string(rng))
    if kind == 'bool':
        return rng.choice(['true', 'false'])
    if kind == 'null':
        return 'null'
    n = rng.choice([0, 1, 1, 2, 2, 3] if depth else
                   [0, 0, 1, 2, 3, 4, 8, 20])
    if kind == 'list':
        items = [rand_json_text(rng, depth + 1) for _ in range(n)]
        return '[' + sp() + (',' + sp()).join(items) + sp() + ']'
    keys = [rand_string(rng)[:30] for _ in range(n)]
    items = [enc_string(rng, k) + sp() + ':' + sp() +
             rand_json_text(rng, depth + 1) for k in keys]
    return '{' + sp() + (',' + sp()).join(items) + sp() + '}'


# --------------------------------------------------------------------------
# app profiles
# --------------------------------------------------------------------------
def make_app(profile, broker):
    from celery import Celery
    from kombu import Exchange, Queue

    app = Celery('golden_%s_%s' % (profile, broker.split(':')[0]),
                 broker=broker, set_as_current=False)
    app.conf.result_backend = None
    app.conf.broker_connection_retry_on_startup = True
    c = app.conf
    if profile == 'defaults':
        pass
    elif profile == 'routed':
        c.task_queues = [
            Queue('default', Exchange('default'), routing_key='default'),
            Queue('hi', Exchange('hiex', type='topic'), routing_key='hi.#'),
            Queue('media', Exchange('media', type='direct'),
                  routing_key='media.video',
                  queue_arguments={'x-max-priority': 10}),
            Queue('lowdur', Exchange('lowdur', delivery_mode=1,
                                     durable=False),
                  routing_key='lowdur', durable=False),
        ]
        c.task_default_queue = 'default'
        c.task_default_exchange = 'default'
        c.task_default_routing_key = 'default'
        c.task_routes = {
            't.routed': {'queue': 'hi', 'routing_key': 'hi.there'},
            't.glob.*': {'queue': 'media'},
            't.rprio': {'queue': 'media', 'priority': 8},
            't.rex': {'exchange': 'hiex', 'routing_key': 'hi.x'},
            't.rlow': 'lowdur',
        }
        c.timezone = 'Europe/Bucharest'
    elif profile == 'strict':
        c.task_create_missing_queues = False
        c.task_default_priority = 5
        c.task_queue_max_priority = 10
        c.task_default_delivery_mode = 'transient'
        c.task_default_queue = 'work'
        c.timezone = 'America/New_York'
        c.task_routes = ([('t.glob.*', {'queue': 'work', 'priority': 2})],)
    elif profile == 'gzip':
        c.task_compression = 'gzip'
        c.timezone = 'Asia/Kolkata'
        c.broker_transport_options = {'priority_steps': [0, 4, 8],
                                      'sep': ':', 'queue_order_strategy':
                                      'priority'}
    else:
        raise ValueError(profile)

    def t(name, **opts):
        def fn(*a, **k):
            return None
        fn.__name__ = name.replace('.', '_')
        return app.task(name=name, **opts)(fn)

    t('t.plain')
    t('t.opts', queue='hi', priority=3, expires=60)
    t('t.prio0', priority=0)
    t('t.ignore', ignore_result=True)
    t('t.pickle', serializer='pickle')
    t('t.limits', time_limit=30, soft_time_limit=20)
    t('t.rk', routing_key='hi.custom', exchange='hiex')
    t('t.routed')
    t('t.glob.one')
    t('t.rprio', priority=2)
    t('t.rex')
    t('t.rlow')
    t('t.zlib', compression='zlib')
    t('t.exp_float', expires=1.5)
    app.finalize(auto=True)
    app._local.oid = REPLY_TO
    return app


def make_amqp_app(profile):
    app = make_app(profile, AMQP_URL)
    # Publisher confirms make RabbitMQ report an unroutable exchange on the
    # publish that caused it instead of on some later call.
    app.conf.broker_transport_options = dict(
        app.conf.broker_transport_options or {}, confirm_publish=True)
    return app


TASKS = ['t.plain', 't.plain', 't.plain', 't.opts', 't.prio0', 't.ignore',
         't.pickle', 't.limits', 't.rk', 't.routed', 't.glob.one', 't.rprio',
         't.rex', 't.rlow', 't.zlib', 't.exp_float', 'not.registered',
         't.glob.unregistered']


def generators(G):
    """Return generator callables; ``G`` is the gen_golden module."""
    if not (REDIS_URL and AMQP_URL):
        print('gen_messages: GOLDEN_REDIS_URL/GOLDEN_AMQP_URL not set, '
              'message goldens NOT regenerated', file=sys.stderr)
        return []

    def gen_messages():
        import amqp
        import celery.app.amqp
        import celery.app.base
        import kombu.transport.virtual.base as vbase
        import redis
        from django_celery_beat.models import IntervalSchedule, PeriodicTask
        from django_celery_beat.schedulers import DatabaseScheduler, ModelEntry

        sys.path.insert(0, os.path.join(os.path.dirname(
            os.path.abspath(__file__)), '..', '..'))
        from django_gobeat.resolve import build_settings

        ids = {'task': None, 'tag': None}
        celery.app.base.uuid = lambda: ids['task']
        vbase.uuid = lambda: ids['tag']
        celery.app.amqp.anon_nodename = lambda *a, **k: ORIGIN
        PeriodicTask.save = lambda self, *a, **k: None

        # capture what py-amqp is asked to send
        amqp_calls = []

        def wrap(name):
            orig = getattr(amqp.Channel, name)

            def w(self, *a, **k):
                amqp_calls.append((name, a, k))
                return orig(self, *a, **k)
            setattr(amqp.Channel, name, w)
        for n in ('exchange_declare', 'queue_declare', 'queue_bind'):
            wrap(n)
        orig_publish = amqp.Channel._basic_publish

        def publish(self, msg, exchange='', routing_key='', mandatory=False,
                    immediate=False, **kw):
            amqp_calls.append(('publish', (), {
                'exchange': exchange, 'routing_key': routing_key,
                'properties': dict(msg.properties),
                'body': msg.body,
                'wire': base64.b64encode(
                    msg._serialize_properties()).decode('ascii'),
            }))
            return orig_publish(self, msg, exchange, routing_key, mandatory,
                                immediate, **kw)
        amqp.Channel._basic_publish = publish
        amqp.Channel.basic_publish = publish

        rcli = redis.Redis.from_url(REDIS_URL)
        rng = random.Random(7007)
        out = G.Out('messages')
        profiles = ['defaults', 'routed', 'strict', 'gzip']
        per_profile = G.N(1500)

        for profile in profiles:
            # kombu caches connections (and what was declared on them) per
            # broker URL process-wide; every profile starts clean, like a
            # freshly started beat.
            import kombu.pools
            kombu.pools.reset()
            rcli.flushdb()
            rapp = make_app(profile, REDIS_URL)
            aapp = make_amqp_app(profile)
            # clean broker state so declarations start from scratch
            with aapp.connection_for_write() as conn:
                ch = conn.default_channel
                for q in ('celery', 'default', 'hi', 'media', 'lowdur', 'work',
                          'brandnew', 'other'):
                    ch.queue_delete(q)
                for ex in ('celery', 'default', 'hiex', 'media', 'lowdur',
                           'work', 'brandnew', 'other', 'unknownex'):
                    ch.exchange_delete(ex)
            ids['task'] = 'gobeat-sync'
            settings_doc = build_settings(rapp, extra_task_names=[
                'not.registered', 't.glob.unregistered'])
            settings_doc['broker_url'] = None
            # keep the golden file independent of the library versions so
            # that matrix.sh can compare it byte for byte
            settings_doc['versions'] = {}
            out.add({'profile': profile, 'settings': settings_doc,
                     'origin': ORIGIN, 'reply_to': REPLY_TO})

            rsched = DatabaseScheduler(app=rapp, lazy=True)
            asched = DatabaseScheduler(app=aapp, lazy=True)

            for i in range(per_profile):
                now = G.rand_now(rng, 'UTC')
                task = rng.choice(TASKS)
                args = rng.choice(['[]', '[]', '', rand_json_text(
                    rng, container='list')])
                kwargs = rng.choice(['{}', '{}', '', rand_json_text(
                    rng, container='dict')])
                headers = rng.choice(['{}', '{}', '{}', '', None, rand_json_text(
                    rng, 1, container='dict'), '{"x-custom": 1}',
                    '{"task": "evil", "id": "evil", "periodic_task_name": "evil"}'])
                queue = rng.choice([None] * 6 + ['hi', 'media', 'default',
                                                 'celery', 'brandnew', 'work',
                                                 'lowdur', ''])
                exchange = rng.choice([None] * 8 + ['hiex', 'default', 'media',
                                                    'unknownex', ''])
                routing_key = rng.choice([None] * 8 + ['hi.a.b', 'media.video',
                                                       'default', 'x', ''])
                priority = rng.choice([None] * 5 + [0, 1, 3, 4, 5, 8, 9, 10,
                                                    200, 255])
                expires = expire_seconds = None
                r = rng.random()
                if r < 0.15:
                    expires = now + timedelta(seconds=rng.choice([
                        rng.uniform(0.5, 86400 * 3), 60, 0.4, 1.0005]))
                    if rng.random() < 0.3:
                        expires = expires.replace(microsecond=0)
                elif r < 0.35:
                    expire_seconds = rng.choice([1, 30, 60, 3600, 86400,
                                                 rng.randint(1, 10 ** 6)])
                name = rng.choice(['pt-%d' % i, 'nightly cleanup',
                                   'ünïcode task', 'q"uoted'])
                pid = rng.randint(1, 10 ** 6)
                ids['task'] = 'task-%s-%d' % (profile, i)
                ids['tag'] = 'tag-%s-%d' % (profile, i)

                rec = {'profile': profile, 'now': G.iso(now),
                       'task_id': ids['task'], 'delivery_tag': ids['tag'],
                       'model': {
                           'id': pid, 'name': name, 'task': task,
                           'args': args, 'kwargs': kwargs, 'headers': headers,
                           'queue': queue, 'exchange': exchange,
                           'routing_key': routing_key, 'priority': priority,
                           'expires': G.iso(expires),
                           'expire_seconds': expire_seconds}}

                def model():
                    return PeriodicTask(
                        id=pid, name=name, task=task, args=args, kwargs=kwargs,
                        headers=headers, queue=queue, exchange=exchange,
                        routing_key=routing_key, priority=priority,
                        expires=expires, expire_seconds=expire_seconds,
                        enabled=True, last_run_at=now, total_run_count=0,
                        interval=IntervalSchedule(every=10, period='seconds'))

                for label in ('redis', 'amqp'):
                    app, sched = ((rapp, rsched) if label == 'redis'
                                  else (aapp, asched))
                    app.now = lambda _n=now, _a=app: _n.astimezone(_a.timezone)
                    del amqp_calls[:]
                    before = {}
                    if label == 'redis':
                        for k in rcli.keys('*'):
                            if rcli.type(k) == b'list':
                                before[k] = rcli.llen(k)
                    try:
                        entry = ModelEntry(model(), app=app)
                        sched.apply_async(entry, advance=False)
                    except Exception as exc:  # noqa: BLE001
                        cause = exc.__cause__ or exc.__context__ or exc
                        rec[label] = {'error': type(cause).__name__,
                                      'detail': str(cause)[:200]}
                        if label == 'amqp' and type(cause).__name__ == 'NotFound':
                            # publish to an exchange nobody declared: the
                            # broker closed the channel; start afresh.
                            aapp.close()
                            aapp = make_amqp_app(profile)
                            asched = DatabaseScheduler(app=aapp, lazy=True)
                        continue
                    if label == 'redis':
                        pushed = []
                        for k in sorted(rcli.keys('*')):
                            if rcli.type(k) != b'list':
                                continue
                            n = rcli.llen(k) - before.get(k, 0)
                            for raw in (rcli.lrange(k, 0, n - 1) if n > 0
                                        else []):
                                pushed.append({
                                    'key': k.decode('utf-8'),
                                    'payload': raw.decode('utf-8')})
                        bindings = {}
                        for k in sorted(rcli.keys('_kombu.binding.*')):
                            bindings[k.decode()] = sorted(
                                m.decode() for m in rcli.smembers(k))
                        rec['redis'] = {'pushed': pushed, 'bindings': bindings}
                    else:
                        calls = []
                        for cname, a, k in amqp_calls:
                            k = dict(k)
                            k.pop('nowait', None)
                            if cname == 'publish':
                                body = k.pop('body')
                                if isinstance(body, str):
                                    body = body.encode('utf-8')
                                k['body_b64'] = base64.b64encode(
                                    body).decode('ascii')
                            calls.append({'call': cname, 'args': k})
                        rec['amqp'] = {'calls': calls}
                out.add(rec)
            rapp.close()
            aapp.close()
        out.close()

    def gen_pyjson():
        """json round trip and repr, straight from kombu and celery."""
        from celery.utils.saferepr import saferepr
        from kombu.utils.json import dumps, loads

        rng = random.Random(8008)
        out = G.Out('pyjson')
        for i in range(G.N(6000)):
            text = rand_json_text(rng, container=rng.choice(
                [None, 'list', 'dict']))
            if rng.random() < 0.08:    # damage it
                pos = rng.randrange(len(text) + 1)
                text = text[:pos] + rng.choice(
                    [',', '}', ']', '"', '\\', 'x', '']) + text[pos + rng.choice([0, 1]):]
            rec = {'text': text}
            try:
                value = loads(text)
            except Exception as exc:  # noqa: BLE001
                rec['error'] = type(exc).__name__
                out.add(rec)
                continue
            rec['dumps'] = dumps(value)
            rec['repr1024_json'] = json.dumps(saferepr(value, 1024))
            rec['repr40_json'] = json.dumps(saferepr(value, 40))
            out.add(rec)
        out.close()

    return [gen_pyjson, gen_messages]
