ARG PG_VERSION=16.3
ARG PG_MAJOR_VERSION=16
ARG VERSION=custom

FROM golang:1.20 AS builder

WORKDIR /go/src/github.com/fly-apps/fly-postgres
COPY . .

RUN CGO_ENABLED=0 GOOS=linux \
    go build -v -o /fly/bin/event_handler ./cmd/event_handler && \
    go build -v -o /fly/bin/failover_validation ./cmd/failover_validation && \
    go build -v -o /fly/bin/pg_unregister ./cmd/pg_unregister && \
    go build -v -o /fly/bin/start_monitor ./cmd/monitor && \
    go build -v -o /fly/bin/start_admin_server ./cmd/admin_server && \
    go build -v -o /fly/bin/start ./cmd/start && \
    go build -v -o /fly/bin/flexctl ./cmd/flexctl


COPY ./bin/* /fly/bin/

FROM ubuntu:24.04

ENV PGDATA=/data/postgresql
ENV PGPASSFILE=/data/.pgpass
ARG VERSION
ARG PG_MAJOR_VERSION
ARG POSTGIS_MAJOR=3
ARG HAPROXY_VERSION=2.8
ARG REPMGR_VERSION=5.4.1-1build2

LABEL fly.app_role=postgres_cluster
LABEL fly.version=${VERSION}
LABEL fly.pg-version=${PG_VERSION}
LABEL fly.pg-manager=repmgr

RUN apt-get update && apt-get install --no-install-recommends -y \
    ca-certificates iproute2 curl bash dnsutils vim socat procps ssh gnupg rsync barman-cli barman barman-cli-cloud python3-setuptools cron gosu \
    && apt autoremove -y && apt clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install PostgreSQL
RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/postgresql-archive-keyring.gpg] http://apt.postgresql.org/pub/repos/apt/ noble-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    apt-get update && apt-get install --no-install-recommends -y \
        postgresql-${PG_MAJOR_VERSION} \
        postgresql-client-${PG_MAJOR_VERSION} \
        postgresql-contrib-${PG_MAJOR_VERSION}

# Repmgr
RUN curl -L http://launchpadlibrarian.net/722514158/postgresql-${PG_MAJOR_VERSION}-repmgr_${REPMGR_VERSION}_amd64.deb -o postgresql-${PG_MAJOR_VERSION}-repmgr_${REPMGR_VERSION}_amd64.deb && \
    apt-get install -y ./postgresql-${PG_MAJOR_VERSION}-repmgr_${REPMGR_VERSION}_amd64.deb \
    && rm ./postgresql-${PG_MAJOR_VERSION}-repmgr_${REPMGR_VERSION}_amd64.deb

# PostGIS
RUN apt-get update && apt-get install --no-install-recommends -y \
    postgresql-${PG_MAJOR_VERSION}-postgis-$POSTGIS_MAJOR \
    postgresql-${PG_MAJOR_VERSION}-postgis-$POSTGIS_MAJOR-scripts

# Haproxy
RUN apt-get update && apt-get install --no-install-recommends -y \
    haproxy=$HAPROXY_VERSION.\* \
    && apt autoremove -y && apt clean


# Add PostgreSQL bin directory to PATH
ENV PATH="/usr/lib/postgresql/${PG_MAJOR_VERSION}/bin:$PATH"

# Copy Go binaries from the builder stage
COPY --from=builder /fly/bin/* /usr/local/bin

# Copy Postgres exporter
COPY --from=wrouesnel/postgres_exporter:latest /postgres_exporter /usr/local/bin/

ADD /config/* /fly/
RUN mkdir -p /run/haproxy/
RUN usermod -d /data postgres

EXPOSE 5432

CMD ["start"]
