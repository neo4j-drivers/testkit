# syntax=docker/dockerfile:1.4
FROM ubuntu:22.04

ARG BUILD_RUST_STUB=true

WORKDIR /root/build/boltstub
RUN --mount=from=boltstub,target=/tmp/boltstub <<EOF
set -e
if [ "$BUILD_RUST_STUB" = "true" ]; then
    apt-get update
    apt-get install -y python3 python3-dev gcc curl
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

FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y \
        git \
        tzdata \
        python3 \
        python3-pip \
        python3-venv \
        golang \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /root/build/boltstub-bin /usr/local/bin/boltstub
ENV PYTHON=python3
RUN python3 -m venv /root/.venv
ENV VIRTUAL_ENV=/root/.venv
ENV PATH="/root/.venv/bin:${PATH}"
