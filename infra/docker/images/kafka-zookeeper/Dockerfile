FROM kafka-base:3.2.1-1.0.0
LABEL manteiner="alex carvalho" \
      version="1.0.0" \
      description="kafka zookeeper"

VOLUME ["/tmp/zookeeper"]

# Just to indicate that this container needs port map
EXPOSE 2181

ENTRYPOINT [ "zookeeper-server-start.sh", "/opt/kafka_2.13-3.2.1/config/zookeeper.properties" ]