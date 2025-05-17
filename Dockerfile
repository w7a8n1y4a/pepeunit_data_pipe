FROM golang:1.24.3 as builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o data_pipe .

FROM alpine:3.19

WORKDIR /app

COPY --from=builder /app/data_pipe .

CMD ["./data_pipe"]

