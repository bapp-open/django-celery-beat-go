#!/bin/sh
# Build the two images the integration tests use.
#   CELERY_VERSION / DCB_VERSION select the Python stack (version matrix).
# Behind a TLS-intercepting proxy set BUILD_CA_FILE (and HTTPS_PROXY).
set -eu
cd "$(dirname "$0")/../.."
CELERY_VERSION="${CELERY_VERSION:-5.5.3}"
DCB_VERSION="${DCB_VERSION:-2.8.1}"
set --
if [ -n "${BUILD_CA_FILE:-}" ]; then
    set -- --network host --secret "id=ca,src=${BUILD_CA_FILE}" \
        --build-arg "HTTPS_PROXY=${HTTPS_PROXY:-}"
fi
docker build -q "$@" \
    --build-arg "CELERY_VERSION=${CELERY_VERSION}" --build-arg "DCB_VERSION=${DCB_VERSION}" \
    -f tests/integration/Dockerfile.django \
    -t "gobeat-it-django:${CELERY_VERSION}-${DCB_VERSION}" .
docker build -q -t gobeat:it .
