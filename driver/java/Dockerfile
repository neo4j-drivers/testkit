# Install Maven 3.6, Java 11, Java 8 and Python3
FROM maven:3.6.3-openjdk-8

RUN apt-get --quiet --quiet update \
    && apt-get --quiet --quiet install -y bash python3 \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHON=python3
ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH=$JAVA_HOME/bin:$PATH

# For Linux Debian
ADD trustedRoot.crt /usr/local/share/ca-certificates/the_host.crt
RUN update-ca-certificates
# JAVA_HOME needed by hook
# ADD CA file to /usr/local/share/ca-certificates/<hostname>/<cert>.crt
# ADD CA file to /usr/local/share/ca-certificates/<cert>.crt
# RUN update-ca-certificates
# pem to crt: openssl x509 x.pem -inform PEM out y.crt
