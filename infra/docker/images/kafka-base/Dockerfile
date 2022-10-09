# Base Image
FROM ubuntu:jammy
LABEL manteiner="alex carvalho"

# Arguments for docker build
ARG SCALA_VERSION=2.13
ARG KAFKA_VERSION=3.2.1
ARG KAFKA_HOME=/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}

ENV KAFKA_HOME=${KAFKA_HOME} \
    PATH="$PATH:${KAFKA_HOME}/bin" \
    SCALA_VERSION=${SCALA_VERSION} \
    KAFKA_VERSION=${KAFKA_VERSION}

# Install jdk 11 and some auxilary linux packages
RUN apt update && \
    apt upgrade -y && \
    apt install -y \
    curl \
    netcat \
    openjdk-11-jdk \
    vim && \
    apt autoremove && \
    apt clean

# Download, unzip, place and config KAFKA
RUN curl -o /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz \
         https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    tar -xf /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt && \
    rm /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    echo "# enable zookeeper status check with 'echo ruok | nc localhost:2181 | grep imok' - by alex" >> $KAFKA_HOME/config/zookeeper.properties && \
    echo "4lw.commands.whitelist=*" >> $KAFKA_HOME/config/zookeeper.properties

VOLUME ["$KAFKA_HOME/config"]
