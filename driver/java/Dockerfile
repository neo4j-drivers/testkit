# Install Maven 3.6, Java 11, Java 8 and Python3
FROM maven:3.6-jdk-8

# These two paths should be the same as TEAMCITY %openjdk-11% and %openjdk-8%
# as Java driver uses them to set NEO4J_HOME and JAVA_HOME
ARG JAVA11=/usr/lib/jvm/openjdk-11
ARG JAVA8=/usr/lib/jvm/openjdk-8

RUN apt-get --quiet --quiet update \
    && apt-get --quiet --quiet install -y bash python3 openjdk-11-jdk

RUN PYTHON=python3 \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/local/openjdk-8 $JAVA8 \
    && rm -rf $JAVA11 \
    && ln -s /usr/lib/jvm/java-11-openjdk-amd64 $JAVA11 \
    && ln -s /usr/local/openjdk-8 $JAVA8 \
    && rm -rf $JAVA11 \
    && ln -s /usr/lib/jvm/java-11-openjdk-amd64 $JAVA11

ENV JAVA_HOME=$JAVA8
ENV PATH=$JAVA_HOME/bin:$PATH
