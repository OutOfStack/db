APP=db

.PHONY: build build-db build-cli run run-cli test lint clean generate docker-build docker-run

build: build-db build-cli

build-db:
	mkdir -p bin
	go build -o bin/$(APP) ./cmd/db

build-cli:
	mkdir -p bin
	go build -o bin/$(APP)-cli ./cmd/db-cli

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
	docker build -t db .

docker-run: docker-build
	docker run --rm -p 127.0.0.1:3223:3223 -e DB_ADDRESS=0.0.0.0:3223 -e DB_ALLOW_REMOTE=true db

generate:
	go tool mockgen -source=internal/compute/compute.go -destination=internal/compute/mocks/compute.go -package=compute_mocks
	go tool mockgen -source=internal/storage/storage.go -destination=internal/storage/mocks/storage.go -package=storage_mock
