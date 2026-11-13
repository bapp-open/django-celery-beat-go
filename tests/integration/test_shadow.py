"""Shadow comparison: Python beat (authoritative) and gobeat in dry-run
mode read the same database; their dispatch logs must agree."""
import os
import time

from conftest import HERE, compose, ctl, healthz, sh, unique, wait_for

GOBEAT_BIN = os.environ.get('GOBEAT_BIN', os.path.join(HERE, '..', '..', 'bin', 'gobeat'))


def test_python_beat_and_gobeat_dry_run_agree():
    compose('stop', '-t', '5', 'gobeat')      # Python beat is the scheduler now
    try:
        ctl({'op': 'create', 'name': unique('shadow-3s'), 'task': 'runlog.tasks.record',
             'interval': [3, 'seconds']},
            {'op': 'create', 'name': unique('shadow-5s'), 'task': 'runlog.tasks.routed',
             'interval': [5, 'seconds'], 'args': [1]},
            {'op': 'create', 'name': unique('shadow-7s'), 'task': 'runlog.tasks.record',
             'interval': [7, 'seconds']},
            {'op': 'create', 'name': unique('shadow-cron'), 'task': 'runlog.tasks.record',
             'crontab': {'minute': '*', 'hour': '*', 'day_of_week': '*',
                         'day_of_month': '*', 'month_of_year': '*',
                         'timezone': 'Asia/Tokyo'}})
        compose('--profile', 'shadow', 'up', '-d', '--no-build', 'pybeat', 'gobeat-shadow')
        wait_for(lambda: 'Sending due task' in compose('logs', 'pybeat').stdout,
                 timeout=60, what='python beat dispatching')
        time.sleep(100)
        # an edit while both run: both must pick it up
        ctl({'op': 'create', 'name': unique('shadow-late'), 'task': 'runlog.tasks.record',
             'interval': [2, 'seconds']})
        time.sleep(40)

        art = os.path.join(HERE, '.artifacts')
        os.makedirs(art, exist_ok=True)
        py_log = os.path.join(art, 'shadow-python-beat.log')
        gb_log = os.path.join(art, 'shadow-gobeat.log')
        with open(py_log, 'w') as fh:
            fh.write(compose('logs', '--no-log-prefix', 'pybeat').stdout)
            fh.write(compose('logs', '--no-log-prefix', 'pybeat').stderr)
        with open(gb_log, 'w') as fh:
            fh.write(compose('logs', '--no-log-prefix', 'gobeat-shadow').stdout)

        # the dry run must not have written anything
        assert 'dry_run":true' in open(gb_log).read()
        p = sh(GOBEAT_BIN, 'shadow-diff', '--python-log', py_log, '--gobeat-log', gb_log,
               '--python-tz', 'Europe/Bucharest', '--tolerance', '2s', '--trim', '6s',
               check=False)
        print(p.stdout)
        assert p.returncode == 0, p.stdout + p.stderr
        matched = int(p.stdout.split('matched:')[1].split()[0])
        assert matched >= 60, p.stdout
    finally:
        compose('--profile', 'shadow', 'rm', '-sf', 'pybeat', 'gobeat-shadow', check=False)
        compose('up', '-d', '--no-build', 'gobeat')
        wait_for(lambda: healthz()[0] == 200, timeout=60, what='gobeat healthy')
