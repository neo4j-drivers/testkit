FROM ubuntu:24.04 AS build

WORKDIR /root/build/boltstub
RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-dev \
        gcc \
        curl \
    && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN --mount=from=boltstub,target=/tmp/boltstub cp -r /tmp/boltstub/* .
RUN cargo build --release

FROM ubuntu:24.04

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
COPY --from=build /root/build/boltstub/target/release/boltstub /usr/local/bin/boltstub
RUN python3 -m venv /root/.venv
ENV VIRTUAL_ENV=/root/.venv
ENV PATH="/root/.venv/bin:${PATH}"

ENV PYTHON=python3
