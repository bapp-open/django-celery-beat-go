# syntax=docker/dockerfile:1
# gobeat: static binary on an empty base image.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY cmd ./cmd
COPY internal ./internal
ARG VERSION=dev
# Dependencies are vendored: the build needs no network access.
RUN CGO_ENABLED=0 GOFLAGS=-mod=vendor go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" -o /gobeat ./cmd/gobeat

FROM scratch
COPY --from=build /gobeat /gobeat
# TLS roots for rediss:// / amqps:// / sslmode=verify-full
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER 65534:65534
EXPOSE 9099
HEALTHCHECK --interval=15s --timeout=4s --start-period=20s --retries=3 CMD ["/gobeat", "healthcheck"]
ENTRYPOINT ["/gobeat"]
CMD ["run"]
