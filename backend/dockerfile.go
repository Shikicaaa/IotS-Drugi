FROM golang:1.24.4-alpine AS builder

WORKDIR /app

COPY go.mod .
RUN go mod tidy

COPY scenario_d.go .

RUN CGO_ENABLED=0 GOOS=linux go build -o scenario_d .

FROM alpine:3.19

WORKDIR /app
COPY --from=builder /app/scenario_d .

ENTRYPOINT ["./scenario_d"]