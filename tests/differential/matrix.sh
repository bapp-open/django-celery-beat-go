#!/bin/sh
# Version matrix: regenerate every golden file with each supported
# Celery / django-celery-beat combination and require the result to be
# byte-identical to the committed files (generated with the pinned
# versions). A difference means that version behaves differently and is
# not supported by this gobeat build.
#
#   GOLDEN_REDIS_URL=... GOLDEN_AMQP_URL=... tests/differential/matrix.sh
set -eu
cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"
WORK="${MATRIX_WORKDIR:-$ROOT/tests/integration/.artifacts/matrix}"
COMBOS="${MATRIX:-5.5.2:2.8.0 5.5.3:2.8.1 5.6.0:2.8.1 5.6.3:2.9.0}"
PY="${PYTHON:-python3}"
status=0
for combo in $COMBOS; do
    celery="${combo%%:*}"; dcb="${combo##*:}"
    venv="$WORK/venv-$celery-$dcb"; out="$WORK/golden-$celery-$dcb"
    mkdir -p "$out"
    if [ ! -x "$venv/bin/python" ]; then
        "$PY" -m venv "$venv"
        "$venv/bin/pip" install -q "celery[redis]==$celery" "django-celery-beat==$dcb" time-machine packaging
    fi
    "$venv/bin/python" gen_golden.py --out "$out" >/dev/null
    "$venv/bin/python" gen_golden.py --out "$out" --use-tz 0 >/dev/null
    for f in "$ROOT"/testdata/golden/*.jsonl.gz; do
        name="$(basename "$f")"
        if [ ! -f "$out/$name" ]; then
            echo "celery $celery + django-celery-beat $dcb: $name was not generated (brokers not configured?)"; status=1
        elif cmp -s "$f" "$out/$name"; then
            echo "celery $celery + django-celery-beat $dcb: $name identical"
        else
            echo "celery $celery + django-celery-beat $dcb: $name DIFFERS"; status=1
        fi
    done
done
exit $status
