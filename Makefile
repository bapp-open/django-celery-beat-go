# gobeat - everything CI does is reachable from here; `make ci` runs it all.
PYTHON      ?= python3
VENV        ?= .venv
PY          := $(VENV)/bin/python
FUZZTIME    ?= 20s
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
GOLDEN_REDIS_URL ?= redis://127.0.0.1:6379/5
GOLDEN_AMQP_URL  ?= amqp://guest:guest@127.0.0.1:5672//
export GOLDEN_REDIS_URL GOLDEN_AMQP_URL

.PHONY: ci build test fuzz venv golden soak-golden soak-check matrix images integration docker

## ci: the whole suite - unit, differential, fuzz, soak, race, leaks, matrix, integration, failure injection, shadow
ci: test fuzz matrix soak-check integration

build:
	CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/gobeat ./cmd/gobeat

## test: unit + differential (golden) + soak tests, with the race detector and goroutine-leak checks
test:
	test -z "$$(gofmt -l cmd internal)"
	go vet ./...
	go test -race -count=1 -timeout 30m ./...
	go test -count=1 -timeout 30m -run 'TestSoak10kTasks30Days' ./internal/scheduler

## fuzz: run every fuzz target for FUZZTIME
fuzz:
	go test -run '^$$' -fuzz FuzzParseField -fuzztime $(FUZZTIME) ./internal/schedule
	go test -run '^$$' -fuzz FuzzCrontabIsDue -fuzztime $(FUZZTIME) ./internal/schedule
	go test -run '^$$' -fuzz FuzzLoads -fuzztime $(FUZZTIME) ./internal/pyjson
	go test -run '^$$' -fuzz FuzzBuild -fuzztime $(FUZZTIME) ./internal/message

venv: $(PY)
$(PY):
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -q -r tests/requirements.txt

## golden: regenerate testdata/golden from real Celery (needs Redis and RabbitMQ, see GOLDEN_*_URL)
golden: venv
	cd tests/differential && ../../$(PY) gen_golden.py --out ../../testdata/golden
	cd tests/differential && ../../$(PY) gen_golden.py --out ../../testdata/golden --use-tz 0

## soak-golden: regenerate the 30-day Python beat reference (about an hour) and the small one
soak-golden: venv
	$(PY) tests/differential/gen_soak.py --out testdata/soak --tasks 100 --days 3
	$(PY) tests/differential/gen_soak.py --out testdata/soak --tasks 1000 --days 30

## soak-check: regenerate the small Python beat reference and require it to equal the committed one
soak-check: venv
	mkdir -p tests/integration/.artifacts/soak
	$(PY) tests/differential/gen_soak.py --out tests/integration/.artifacts/soak --tasks 100 --days 3
	cmp testdata/soak/soak_100_3d.json.gz tests/integration/.artifacts/soak/soak_100_3d.json.gz

## matrix: regenerate all goldens under every supported Celery/django-celery-beat version; must be byte-identical
matrix:
	PYTHON=$(PYTHON) tests/differential/matrix.sh

images:
	tests/integration/build.sh

## integration: docker compose (Postgres, Redis, RabbitMQ, Django, workers): scenarios, failure injection, shadow diff - on both brokers
integration: build images venv
	cd tests/integration && BROKER=redis ../../$(PY) -m pytest -x -q test_integration.py test_failures.py test_shadow.py
	cd tests/integration && BROKER=amqp  ../../$(PY) -m pytest -x -q test_integration.py test_failures.py test_shadow.py

docker:
	docker build --build-arg VERSION=$(VERSION) -t gobeat:$(VERSION) .
