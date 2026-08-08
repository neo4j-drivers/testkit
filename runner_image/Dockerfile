# syntax=docker/dockerfile:1.4
FROM ubuntu:20.04 AS build

ARG BUILD_RUST_STUB=true

WORKDIR /root/build/boltstub
RUN --mount=from=boltstub,target=/tmp/boltstub <<EOF
set -e
if [ "$BUILD_RUST_STUB" = "true" ]; then
    apt-get update
    apt-get install -y python3 python3-dev python3-pip curl
    rm -rf /var/lib/apt/lists/*
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    export PATH="/root/.cargo/bin:$PATH"
    cp -r /tmp/boltstub/* .
    cargo build --release
    cp target/release/boltstub /root/build/boltstub-bin
else
    touch /root/build/boltstub-bin
    chmod +x /root/build/boltstub-bin
fi
EOF

FROM ubuntu:20.04

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y \
        git \
        tzdata \
        python3 \
        python3-pip \
        golang \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /root/build/boltstub-bin /usr/local/bin/boltstub

ENV PYTHON=python3
