FROM postgres:14-alpine

ENV PLUGIN_VERSION=v1.5.4.Final

RUN apk add --no-cache protobuf-c-dev
# Compile the plugins from sources and install
RUN apk add --no-cache --virtual .debezium-build-deps gcc clang llvm git make musl-dev pkgconf \
    && git clone https://github.com/debezium/postgres-decoderbufs -b $PLUGIN_VERSION --single-branch \
    && (cd /postgres-decoderbufs && make && make install) \
    && rm -rf postgres-decoderbufs \
    && git clone https://github.com/eulerto/wal2json -b master --single-branch \
    && (cd /wal2json && make && make install) \
    && rm -rf wal2json \
    && apk del .debezium-build-deps

# Copy the custom configuration which will be passed down to the server (using a .sample file is the preferred way of doing it by 
# the base Docker image)
COPY ./postgres.conf /usr/local/share/postgresql/postgresql.conf.sample
# Copy the script which will initialize the replication permissions
COPY ./docker-entrypoint-initdb.d /docker-entrypoint-initdb.d