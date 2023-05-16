ARG PG_VERSION=15.2
ARG PG_MAJOR_VERSION=15
ARG VERSION=custom

FROM golang:1.20

WORKDIR /go/src/github.com/fly-examples/fly-postgres
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/event_handler ./cmd/event_handler
RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/failover_validation ./cmd/failover_validation
RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/pg_unregister ./cmd/pg_unregister
RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/start_monitor ./cmd/monitor
RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/start_admin_server ./cmd/admin_server
RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/start ./cmd/start

COPY ./bin/* /fly/bin/

FROM wrouesnel/postgres_exporter:latest AS postgres_exporter
FROM postgres:${PG_VERSION}
ENV PGDATA=/data/postgresql
ARG VERSION
ARG PG_MAJOR_VERSION
ARG POSTGIS_MAJOR=3
ARG HAPROXY_VERSION=2.7

LABEL fly.app_role=postgres_cluster
LABEL fly.version=${VERSION}
LABEL fly.pg-version=${PG_VERSION}
LABEL fly.pg-manager=repmgr

RUN apt-get update && apt-get install --no-install-recommends -y \
    ca-certificates iproute2 postgresql-$PG_MAJOR_VERSION-repmgr curl bash dnsutils vim socat procps ssh gnupg rsync barman-cli \
    && apt autoremove -y

# PostGIS
RUN apt-get update && apt-get install --no-install-recommends -y \
    postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR \
    postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts

# Haproxy
RUN curl https://haproxy.debian.net/bernat.debian.org.gpg \
      | gpg --dearmor > /usr/share/keyrings/haproxy.debian.net.gpg

RUN echo deb "[signed-by=/usr/share/keyrings/haproxy.debian.net.gpg]" \
      http://haproxy.debian.net bullseye-backports-${HAPROXY_VERSION} main \
      > /etc/apt/sources.list.d/haproxy.list

RUN apt-get update && apt-get install --no-install-recommends -y \
    haproxy=$HAPROXY_VERSION.\* \
    && apt autoremove -y

COPY --from=0 /fly/bin/* /usr/local/bin
COPY --from=postgres_exporter /postgres_exporter /usr/local/bin/

ADD /config/* /fly/
RUN mkdir -p /run/haproxy/
RUN usermod -d /data postgres

EXPOSE 5432

CMD ["start"]
