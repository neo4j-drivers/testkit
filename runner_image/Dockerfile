FROM ubuntu:26.04

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

ENV PYTHON=python3
