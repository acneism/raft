FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/raft-node ./cmd/node

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/raft-node /app/raft-node

COPY Raftfile /app/Raftfile

EXPOSE 8001 8002 8003 9001 9002 9003

ENTRYPOINT ["/app/raft-node"]
CMD ["--id", "1", "--env", "production"]
