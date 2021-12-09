ARG PG_VERSION=14.1
ARG VERSION=custom

FROM golang:1.16 as flyutil

WORKDIR /go/src/github.com/fly-examples/fly-postgres
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/flyadmin ./cmd/flyadmin
RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/start ./cmd/start
COPY ./bin/* /fly/bin/

FROM wrouesnel/postgres_exporter:latest AS postgres_exporter



FROM postgres:${PG_VERSION}
ENV PGDATA=/data/pg_data
ARG VERSION 

LABEL fly.app_role=postgres_cluster
LABEL fly.version=${VERSION}
LABEL fly.pg-version=${PG_VERSION}

RUN apt-get update && apt-get install --no-install-recommends -y \
    ca-certificates iproute2 consul curl bash dnsutils vim procps jq \
    && apt autoremove -y

COPY --from=postgres_exporter /postgres_exporter /usr/local/bin/
COPY --from=0 /fly/bin/* /usr/local/bin

EXPOSE 5432


CMD ["start"]
