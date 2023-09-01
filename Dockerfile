FROM golang:1.20-bullseye AS builder
WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -buildmode=c-shared -o out_ydb.so

FROM ghcr.io/fluent/fluent-bit:2.1.8 AS final
COPY --from=builder /build/out_ydb.so /fluent-bit/lib/

ENTRYPOINT ["/fluent-bit/bin/fluent-bit", "-e", "/fluent-bit/lib/out_ydb.so", "-c"]
CMD ["/fluent-bit/etc/fluent-bit.conf"]