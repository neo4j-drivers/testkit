FROM ubuntu:18.04

RUN apt-get update && \
    apt-get install -y \
        git \
        curl \
        python3 \
        firefox \
        nodejs \
        npm \
    && rm -rf /var/lib/apt/lists/*

RUN npm install -g npm \
    && /bin/bash -c 'hash -d npm'
RUN npm install -g gulp
