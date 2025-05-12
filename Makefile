APP=db

.PHONY: build run test clean

build:
	mkdir -p bin
	go build -o bin/$(APP) ./

run: build
	./bin/$(APP)

test:
	go test -v -race ./...

clean:
	rm -rf bin
