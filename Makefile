APP=db

.PHONY: build build-db build-cli run test lint clean

build:
	mkdir -p bin
	go build -o bin/$(APP) cmd/db/main.go
	go build -o bin/$(APP)-cli cmd/db-cli/main.go

build-db:
	mkdir -p bin
	go build -o bin/$(APP) cmd/db/main.go

build-cli:
	mkdir -p bin
	go build -o bin/$(APP)-cli cmd/db-cli/main.go


run: build
	go run ./cmd/db/.

test:
	go test -v -race ./...

lint:
	golangci-lint run

clean:
	rm -rf bin

MOCKGEN_PKG := go.uber.org/mock/mockgen@v0.5
MOCKGEN_BIN := $(shell go env GOPATH)/bin/mockgen
generate:
	@if \[ ! -f ${MOCKGEN_BIN} \]; then \
		echo "Installing mockgen..."; \
		go install ${MOCKGEN_PKG}; \
	fi
	@if \[ -f ${MOCKGEN_BIN} \]; then \
		echo "Found mockgen at '$(MOCKGEN_BIN)', generating mocks..."; \
	else \
		echo "mockgen not found or the file does not exist"; \
		exit 1; \
  	fi
	${MOCKGEN_BIN} -source=internal/compute/compute.go -destination=internal/compute/mocks/compute.go -package=compute_mocks
	${MOCKGEN_BIN} -source=internal/storage/storage.go -destination=internal/storage/mocks/storage.go -package=storage_mock
