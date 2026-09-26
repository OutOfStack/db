FROM golang:1.27-alpine3.24 AS build
# Build-time identity reported by `db -version`; pass --build-arg on a release build. The image has no git history, so
# an unset argument leaves the binary marked as an unreleased build rather than claiming a version.
ARG VERSION=dev
ARG COMMIT=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build \
    -ldflags="-s -w -X github.com/OutOfStack/db/internal/version.release=${VERSION} -X github.com/OutOfStack/db/internal/version.commit=${COMMIT}" \
    -o /out/db ./cmd/db
RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/db /db
# Relative config and data paths resolve from here.
WORKDIR /home/nonroot
# Make the default data directory writable by the runtime user.
COPY --from=build --chown=65532:65532 /out/data ./data
EXPOSE 3223
ENTRYPOINT ["/db"]
