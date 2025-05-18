FROM golang:1.24.3-alpine3.21 as builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build \
    -trimpath \
    -ldflags="-s -w" \
    -o api

FROM alpine:3.20
RUN apk add --no-cache ffmpeg
RUN addgroup -S nonroot && adduser -S nonroot -G nonroot
WORKDIR /home/nonroot/app
COPY --from=builder /app/api .
RUN chown -R nonroot:nonroot /home/nonroot/app
USER nonroot:nonroot
ENTRYPOINT ["./api"]
