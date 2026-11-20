"""Redis Sentinel broker (BROKER=sentinel): master discovery and failover.

gobeat finds the master the way kombu's SentinelChannel does and asks the
Sentinels again on every reconnection; a master that was demoted answers
writes with READONLY (or Sentinel kills its clients), which drops the
connection. Claimed runs wait in the journal meanwhile, so dispatching
resumes on the new master without a restart.

Redis replication is asynchronous: a message the old master accepted
just before it died may never reach the replica. That loss belongs to the
broker (Python beat has it too), so a run may be missing only inside the
failover window; outside it every run executes exactly once, and nothing
ever executes twice.
"""
import time

import pytest

from conftest import (BROKER, compose, ctl, healthz, inspect, journal, psql,
                      runs, unique, wait_for)

pytestmark = pytest.mark.skipif(BROKER != 'sentinel', reason='BROKER=sentinel only')


def master():
    """The master's host as Sentinel reports it (a compose service name)."""
    out = compose('exec', '-T', 'sentinel1', 'redis-cli', '-p', '26379',
                  'sentinel', 'get-master-addr-by-name', 'mymaster').stdout
    return out.split()[0]


def started_at():
    return inspect('gobeat', '{{.State.StartedAt}} {{.RestartCount}} {{.State.Running}}')


def check_runs(name, window):
    """No duplicates; every journal row published; a run that was published
    but never executed only inside `window` (start, end)."""
    wait_for(lambda: journal(name) and all(r['published_at'] for r in journal(name)[:-1]),
             timeout=60, what='journal of %s published' % name)
    time.sleep(4)
    read_at = time.time()
    r = runs(name)
    j = journal(name)
    ids = [x['task_id'] for x in r]
    assert len(ids) == len(set(ids)), 'duplicate executions'
    assert set(ids) <= {x['task_id'] for x in j}, 'executed but never journaled'
    lost = [x for x in j if x['task_id'] not in set(ids)
            and x['claimed_ts'] < read_at - 3
            and not (window[0] - 2 < x['claimed_ts'] < window[1] + 2)]
    assert not lost, 'published but never executed outside the failover window: %r' % lost
    return j, r


def test_sentinel_settings_and_dispatch():
    row = psql("select data->>'broker_url' as url, "
               "data->'broker_transport_options'->>'master_name' as master "
               "from django_gobeat_settings")[0]
    assert row['url'].startswith('sentinel://sentinel1:26379;')
    assert row['master'] == 'mymaster'

    name = unique('sentinel')
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [1, 'seconds']})
    wait_for(lambda: len(runs(name)) >= 4, timeout=30, what='runs via Sentinel')
    check_runs(name, (0, 0))


def failover(trigger, name):
    ctl({'op': 'create', 'name': name, 'task': 'runlog.tasks.record',
         'interval': [1, 'seconds']})
    wait_for(lambda: len(runs(name)) >= 3, timeout=30, what='runs before the failover')
    before, old = started_at(), master()

    start = time.time()
    trigger(old)
    new = wait_for(lambda: master() != old and master(), timeout=60,
                   what='Sentinel to promote the replica')
    end = time.time()

    n = len(runs(name))
    wait_for(lambda: len(runs(name)) >= n + 5, timeout=90,
             what='runs to resume on the new master %s' % new)
    status, _ = healthz()
    assert status == 200
    assert started_at() == before, 'gobeat restarted or crashed'
    check_runs(name, (start, end))
    return old, new


def test_sentinel_graceful_failover():
    old, new = failover(
        lambda old: compose('exec', '-T', 'sentinel1', 'redis-cli', '-p', '26379',
                            'sentinel', 'failover', 'mymaster'),
        unique('failover'))
    assert {old, new} == {'redis', 'redis-replica'}


def test_sentinel_master_crash():
    stopped = []

    def crash(old):
        compose('--profile', 'sentinel', 'kill', old)
        stopped.append(old)

    try:
        failover(crash, unique('crash'))
    finally:
        # bring the old master back; Sentinel turns it into a replica
        for svc in stopped:
            compose('--profile', 'sentinel', 'start', svc)
