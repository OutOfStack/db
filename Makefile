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

run: build-db
	go run ./cmd/db/.

run-cli: build-cli
	go run ./cmd/db-cli/.

test:
	go test -v -race ./...

lint:
	golangci-lint run

clean:
	rm -rf bin

generate:
	go tool mockgen -source=internal/compute/compute.go -destination=internal/compute/mocks/compute.go -package=compute_mocks
	go tool mockgen -source=internal/storage/storage.go -destination=internal/storage/mocks/storage.go -package=storage_mock
