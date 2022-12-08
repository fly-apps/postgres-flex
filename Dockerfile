ARG PG_VERSION=14.6
ARG VERSION=custom

FROM golang:1.16 as flyutil

WORKDIR /go/src/github.com/fly-examples/fly-postgres
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/flyadmin ./cmd/flyadmin
RUN CGO_ENABLED=0 GOOS=linux go build -v -o /fly/bin/start ./cmd/start
COPY ./bin/* /fly/bin/

FROM postgres:${PG_VERSION}
ENV PGDATA=/data/postgresql
ARG VERSION 

LABEL fly.app_role=postgres_cluster
LABEL fly.pg-version=${PG_VERSION}

RUN apt-get update && apt-get install --no-install-recommends -y \
    ca-certificates iproute2 postgresql-14-repmgr curl bash dnsutils vim procps jq \
    && apt autoremove -y

COPY --from=0 /fly/bin/* /usr/local/bin

ADD /config/* /fly/

RUN usermod -d /data postgres

EXPOSE 5432


CMD ["start"]

