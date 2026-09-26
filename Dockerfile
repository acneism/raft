FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/raft-node ./cmd/node

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/raft-node /app/raft-node

EXPOSE 8001 9001

ENTRYPOINT ["/app/raft-node"]
CMD ["--id", "1", "--peers", "1=raft-1.internal:8001,2=raft-2.internal:8001,3=raft-3.internal:8001"]
