APP=db

# Build-time identity printed by `db -version` / `db-cli -version`. Override VERSION for a release build; both fall back
# to values that mark the binary as an unreleased local build.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse HEAD 2>/dev/null || echo unknown)
VERSION_PKG=github.com/OutOfStack/db/internal/version
LDFLAGS=-X $(VERSION_PKG).release=$(VERSION) -X $(VERSION_PKG).commit=$(COMMIT)

.PHONY: build build-db build-cli run run-cli test lint clean generate docker-build docker-run

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

lint:
	golangci-lint run

clean:
	rm -rf bin

docker-build:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t db .

docker-run: docker-build
	docker run --rm -p 127.0.0.1:3223:3223 -e DB_ADDRESS=0.0.0.0:3223 -e DB_ALLOW_REMOTE=true db

generate:
	go tool mockgen -source=internal/compute/compute.go -destination=internal/compute/mocks/compute.go -package=compute_mocks
	go tool mockgen -source=internal/storage/storage.go -destination=internal/storage/mocks/storage.go -package=storage_mock
