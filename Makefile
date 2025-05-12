APP=db

.PHONY: build run test lint clean

build:
	mkdir -p bin
	go build -o bin/$(APP) cmd/db/main.go

run: build
	go run ./cmd/db/.

test:
	go test -v -race ./...

lint:
	golangci-lint run

clean:
	rm -rf bin
