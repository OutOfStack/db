APP=db

# Build-time identity printed by `db -version` / `db-cli -version`. Override VERSION for a release build; both fall back
# to values that mark the binary as an unreleased local build. An injected commit replaces the toolchain's own VCS
# stamp, so COMMIT carries the "+dirty" suffix itself when the tree has uncommitted or untracked changes.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)$(shell test -n "$$(git status --porcelain 2>/dev/null)" && echo +dirty)
VERSION_PKG=github.com/OutOfStack/db/internal/version
LDFLAGS=-X $(VERSION_PKG).release=$(VERSION) -X $(VERSION_PKG).commit=$(COMMIT)

# The linter version CI runs; keep .github/workflows/main.yaml in step with it.
GOLANGCI_LINT_VERSION=v2.14.0

.PHONY: build build-db build-cli run run-cli test restore-drill lint lint-install clean generate \
	docker-build docker-run container-smoke dist release-verify release-check

build: build-db build-cli

build-db:
	mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o bin/$(APP) ./cmd/db

build-cli:
	mkdir -p bin
	go build -ldflags "$(LDFLAGS)" -o bin/$(APP)-cli ./cmd/db-cli

run:
	go run ./cmd/db

run-cli:
	go run ./cmd/db-cli

test:
	go test -v -race ./...

# Offline backup and restore drill against the real binaries (see docs/operations.md). Linux/macOS, needs bash.
restore-drill: build
	./scripts/restore-drill.sh

lint:
	golangci-lint run

lint-install:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

# Release archives and SHA256SUMS for every supported and provided platform (see README Platforms). Without a VERSION
# override the archives carry the git-describe version, which is enough for a dry run.
dist:
	VERSION=$(VERSION) COMMIT=$(COMMIT) ./scripts/dist.sh

clean:
	rm -rf bin dist

docker-build:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t db .

# Start, query, stop with SIGTERM, restart and query the image again (see scripts/container-smoke.sh). Needs Docker.
container-smoke: docker-build build-cli
	./scripts/container-smoke.sh db

docker-run: docker-build
	docker run --rm -p 127.0.0.1:3223:3223 -e DB_ADDRESS=0.0.0.0:3223 -e DB_ALLOW_REMOTE=true db

generate:
	go tool mockgen -source=internal/compute/compute.go \
		-destination=internal/compute/mocks/compute.go -package=compute_mocks
	go tool mockgen -source=internal/storage/storage.go \
		-destination=internal/storage/mocks/storage.go -package=storage_mock

# Build a host archive in dist/verify and run the black-box release checks against it (see test/release/README.md).
# The soak runs 30 seconds unless SOAK_SECONDS is set; 10800 is the three-hour v1 RC soak.
VERIFY_DIST ?= dist/verify
HOST_PLATFORM = $(shell go env GOOS)/$(shell go env GOARCH)
release-verify:
	PLATFORMS=$(HOST_PLATFORM) DIST=$(VERIFY_DIST) VERSION=$(VERSION) COMMIT=$(COMMIT) ./scripts/dist.sh
	VERSION=$(VERSION) ./test/release/run.sh \
		"$(VERIFY_DIST)/db_$(VERSION)_$(subst /,_,$(HOST_PLATFORM)).tar.gz" $(SOAK_SECONDS)

# Complete local gate, including static checks and the container. Requires Docker and GNU coreutils.
release-check: lint docker-build
	go test ./...
	go vet ./...
	go test -race ./...
	IMAGE=db $(MAKE) release-verify
