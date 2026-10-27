# fs-snmp poller image (used by the optional "poller" compose profile and
# for deploying the poller on remote hosts).
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /fs-snmp .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=build /fs-snmp /usr/local/bin/fs-snmp
COPY snmp.json config.yaml ./
ENTRYPOINT ["fs-snmp"]
