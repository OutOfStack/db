FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/db ./cmd/db

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/db /db
# bind all interfaces so the server is reachable through the published port
ENV DB_ADDRESS=0.0.0.0:3223
EXPOSE 3223
ENTRYPOINT ["/db"]
