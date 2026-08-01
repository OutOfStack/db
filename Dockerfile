FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/db ./cmd/db
RUN mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/db /db
# Relative config and data paths resolve from here.
WORKDIR /home/nonroot
# Make the default data directory writable by the runtime user.
COPY --from=build --chown=65532:65532 /out/data ./data
ENV DB_ADDRESS=0.0.0.0:3223
EXPOSE 3223
ENTRYPOINT ["/db"]
