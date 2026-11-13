"""Failure injection against the real stack.

Documented policy (README "Delivery guarantees"):
  * a run is claimed in the database before it is published;
  * a claimed run is never dropped: it is published once the broker is
    reachable again (at-least-once; a crash *after* the broker accepted
    the message but before the journal was marked can deliver that one
    run twice, with the same task id);
  * runs that became due while gobeat or the database was down are caught
    up once (default) or skipped.
Clock jumps are covered with a fake clock in internal/scheduler.
"""
import time

import pytest

from conftest import (BROKER_SERVICE, ENV, assert_exactly_once, compose,
                      container, ctl, healthz, inspect, journal, log_events,
                      logs, psql, runs, sh, unique, wait_for)


def started_at():
    return inspect('gobeat', '{{.State.StartedAt}} {{.RestartCount}} {{.State.Running}}')


def gobeat_env(**extra):
    return dict(ENV, **extra)


@pytest.fixture
def restore_gobeat():
    yield
    compose('--profile', 'replica', 'rm', '-sf', 'gobeat2', check=False)
    compose('up', '-d', '--no-build', '--force-recreate', 'gobeat')
    wait_for(lambda: healthz()[0] == 200, timeout=60, what='gobeat healthy')


def test_database_down_and_back():
    name = unique('dbdown')
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [1, 'seconds']})
    wait_for(lambda: len(runs(name)) >= 3, timeout=20, what='runs')
    before = started_at()

    stopped = time.time()
    compose('stop', '-t', '2', 'postgres')
    time.sleep(8)
    status, body = healthz()
    assert status == 200, 'gobeat must stay alive (retrying) while the DB is down'
    assert body['state'] in ('retrying', 'leader')
    compose('start', 'postgres')
    restarted = time.time()
    wait_for(lambda: psql('select 1 as ok'), timeout=30, what='postgres back')

    n = len(runs(name))
    wait_for(lambda: len(runs(name)) >= n + 3, timeout=40, what='runs resume')
    assert started_at() == before, 'gobeat restarted or crashed'
    assert any('database session failed' in e.get('msg', '') for e in log_events())

    j = journal(name)
    ids = [r['task_id'] for r in runs(name)]
    assert len(ids) == len(set(ids)), 'duplicate executions'
    assert all(x['published_at'] for x in j[:-1])
    # nothing was claimed while the database was unreachable
    assert not [x for x in j if stopped + 2.5 < x['claimed_ts'] < restarted - 0.5]
    # catch-up-once: exactly one run for the whole outage, then the cadence
    after = sorted(x['claimed_ts'] for x in j if x['claimed_ts'] >= restarted - 0.5)
    assert after[1] - after[0] > 0.7, 'burst of catch-up runs: %r' % after[:4]
    # every run claimed outside the outage window was executed exactly once
    # (the worker itself cannot record executions while Postgres is down)
    executed = set(ids)
    missing = [x for x in j[:-2] if x['task_id'] not in executed
               and not (stopped - 3 < x['claimed_ts'] < restarted + 3)]
    assert not missing, missing


def test_broker_down_and_back():
    name = unique('brokerdown')
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [1, 'seconds']})
    wait_for(lambda: len(runs(name)) >= 3, timeout=20, what='runs')
    before = started_at()

    compose('stop', '-t', '5', BROKER_SERVICE)
    stopped = time.time()
    time.sleep(8)
    assert healthz()[0] == 200
    # runs keep being claimed; they wait in the journal
    pending = [x for x in journal(name) if not x['published_at']]
    assert len(pending) >= 3, pending
    compose('start', BROKER_SERVICE)

    wait_for(lambda: all(x['published_at'] for x in journal(name)[:-1]),
             timeout=90, what='backlog published')
    ctl({'op': 'update', 'name': name, 'enabled': False})
    j, r = assert_exactly_once(name, settle=6)
    during = [x for x in j if stopped < x['claimed_ts'] < stopped + 7]
    assert len(during) >= 5, 'runs claimed during the outage must not be lost'
    assert started_at() == before
    assert any('publish failed' in e.get('msg', '') for e in log_events())


def test_sigkill_between_claim_and_publish(restore_gobeat):
    name = unique('kill-claim')
    compose('up', '-d', '--no-build', '--force-recreate', 'gobeat',
            env=gobeat_env(GOBEAT_FAULT_INJECT='after_claim=30s'))
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [2, 'seconds']})
    wait_for(lambda: 'fault injection: pausing' in logs(), timeout=30,
             what='gobeat paused after a claim')
    sh('docker', 'kill', '-s', 'KILL', container('gobeat'))
    j = journal(name)
    assert len(j) == 1 and j[0]['published_at'] is None, j
    assert runs(name) == [], 'nothing may be published before the kill'

    compose('up', '-d', '--no-build', '--force-recreate', 'gobeat')
    wait_for(lambda: journal(name)[0]['published_at'], timeout=30,
             what='orphaned claim published after restart')
    assert any('never published' in e.get('msg', '') for e in log_events())
    ctl({'op': 'update', 'name': name, 'enabled': False})
    j, r = assert_exactly_once(name)
    assert j[0]['task_id'] in {x['task_id'] for x in r}


def test_sigkill_between_publish_and_journal_mark(restore_gobeat):
    name = unique('kill-publish')
    compose('up', '-d', '--no-build', '--force-recreate', 'gobeat',
            env=gobeat_env(GOBEAT_FAULT_INJECT='after_publish=30s'))
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [2, 'seconds']})
    wait_for(lambda: 'fault injection: pausing' in logs(), timeout=30,
             what='gobeat paused after a publish')
    wait_for(lambda: runs(name), timeout=15, what='first delivery executed')
    sh('docker', 'kill', '-s', 'KILL', container('gobeat'))
    first = journal(name)[0]
    assert first['published_at'] is None

    compose('up', '-d', '--no-build', '--force-recreate', 'gobeat')
    wait_for(lambda: journal(name)[0]['published_at'], timeout=30,
             what='journal row settled')
    ctl({'op': 'update', 'name': name, 'enabled': False})
    time.sleep(5)
    ids = [x['task_id'] for x in runs(name)]
    # documented at-least-once edge: this one run may be delivered twice,
    # and then both deliveries carry the same task id
    assert 1 <= ids.count(first['task_id']) <= 2
    others = [i for i in ids if i != first['task_id']]
    assert len(others) == len(set(others))
    assert set(ids) == {x['task_id'] for x in journal(name)}


def test_network_partition(restore_gobeat):
    name = unique('partition')
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [1, 'seconds']})
    wait_for(lambda: len(runs(name)) >= 3, timeout=20, what='runs')
    before = started_at()
    cid = container('gobeat')
    net = 'gobeat-it_default'

    sh('docker', 'network', 'disconnect', net, cid)
    cut = time.time()
    time.sleep(12)
    assert inspect('gobeat', '{{.State.Running}}') == 'true'
    sh('docker', 'network', 'connect', '--alias', 'gobeat', net, cid)
    healed = time.time()

    n = len(runs(name))
    wait_for(lambda: len(runs(name)) >= n + 3, timeout=90, what='runs resume')
    assert started_at() == before, 'gobeat restarted or crashed'
    ctl({'op': 'update', 'name': name, 'enabled': False})
    j, r = assert_exactly_once(name, settle=6)
    # isolated from Postgres nothing can be claimed, so nothing can be sent
    assert not [x for x in j if cut + 6 < x['claimed_ts'] < healed - 0.5]


def test_two_instances_one_leader_and_failover(restore_gobeat):
    name = unique('replicas')
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [1, 'seconds']})
    compose('--profile', 'replica', 'up', '-d', '--no-build', 'gobeat2')
    wait_for(lambda: 'standing by' in logs('gobeat2'), timeout=30,
             what='second replica standing by')
    wait_for(lambda: len(runs(name)) >= 5, timeout=20, what='runs')
    assert 'dispatch' not in [e.get('msg') for e in log_events('gobeat2')]

    killed = time.time()
    sh('docker', 'kill', '-s', 'KILL', container('gobeat'))
    wait_for(lambda: 'acquired leader lock' in logs('gobeat2'), timeout=20,
             what='standby takes over')
    n = len(runs(name))
    wait_for(lambda: len(runs(name)) >= n + 4, timeout=30,
             what='runs continue under the new leader')
    took = min(x['claimed_ts'] for x in journal(name) if x['claimed_ts'] > killed)
    assert took - killed < 10, 'failover took %.1fs' % (took - killed)

    # the old leader comes back and must stand by
    compose('up', '-d', '--no-build', 'gobeat')
    wait_for(lambda: 'standing by' in logs('gobeat', since='20s'), timeout=30,
             what='restarted instance standing by')
    time.sleep(4)
    ctl({'op': 'update', 'name': name, 'enabled': False})
    j, r = assert_exactly_once(name, settle=5)
    sched = sorted(x['scheduled_ts'] for x in j)
    assert all(b - a > 0.5 for a, b in zip(sched, sched[1:])), \
        'two instances dispatched the same slot'


@pytest.mark.parametrize('policy', ['catch_up_once', 'skip'])
def test_missed_runs_after_downtime(restore_gobeat, policy):
    name = unique('missed-' + policy)
    compose('up', '-d', '--no-build', '--force-recreate', 'gobeat',
            env=gobeat_env(GOBEAT_MISSED_RUN_POLICY=policy,
                           GOBEAT_MISSED_RUN_GRACE='3s'))
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [4, 'seconds']})
    wait_for(lambda: len(runs(name)) >= 1, timeout=20, what='first run')
    compose('stop', '-t', '5', 'gobeat')
    last = max(x['claimed_ts'] for x in journal(name))
    time.sleep(13)                      # three runs are missed
    compose('start', 'gobeat')
    up = time.time()
    wait_for(lambda: any(e.get('msg') == 'scheduler started' and
                         e.get('missed_run_policy') == policy
                         for e in log_events()[-60:]), timeout=20, what='restart')
    time.sleep(7)
    new = sorted(x['claimed_ts'] for x in journal(name) if x['claimed_ts'] > last)
    if policy == 'catch_up_once':
        # one immediate run for the whole gap, then the normal cadence
        assert new and new[0] - up < 2.5, new
        assert len(new) >= 2 and 3.0 < new[1] - new[0] < 5.5, new
    else:
        skipped = [e for e in log_events() if 'skipping missed run' in e.get('msg', '')
                   and e.get('name') == name]
        assert skipped, 'the skip was not logged'
        # nothing right after start-up; the next run is one interval later
        assert new and 3.0 < new[0] - up < 6.5, [n - up for n in new]
    assert_exactly_once(name, settle=2)
