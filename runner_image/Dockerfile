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

ENV PYTHON=python3
