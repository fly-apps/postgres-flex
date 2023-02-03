ARG PG_VERSION=15.1
ARG PG_MAJOR_VERSION=15
ARG VERSION=custom

FROM golang:1.19 as flyutil

WORKDIR /go/src/github.com/fly-examples/fly-postgres
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/event_handler ./cmd/event_handler
RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/failover_validation ./cmd/failover_validation
RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/standby_cleaner ./cmd/standby_cleaner
RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/pg_unregister ./cmd/pg_unregister
RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/start_admin_server ./cmd/admin_server

RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/start ./cmd/start

COPY ./bin/* /fly/bin/

FROM wrouesnel/postgres_exporter:latest AS postgres_exporter

FROM postgres:${PG_VERSION}
ENV PGDATA=/data/postgresql
ARG VERSION
ARG PG_MAJOR_VERSION

LABEL fly.app_role=postgres_cluster
LABEL fly.version=${VERSION}
LABEL fly.pg-version=${PG_VERSION}
LABEL fly.pg-manager=repmgr

RUN apt-get update && apt-get install --no-install-recommends -y \
    ca-certificates iproute2 postgresql-$PG_MAJOR_VERSION-repmgr curl bash dnsutils vim procps jq pgbouncer ssh \
    && apt autoremove -y

COPY --from=0 /fly/bin/* /usr/local/bin
COPY --from=postgres_exporter /postgres_exporter /usr/local/bin/

ADD /config/* /fly/
RUN mkdir -p /run/haproxy/
RUN usermod -d /data postgres

EXPOSE 5432


CMD ["start"]
