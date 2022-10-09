FROM kafka-base:3.2.1-1.0.0
LABEL manteiner="alex carvalho" \
      version="1.0.0" \
      description="kafka server"

VOLUME ["/tmp/kafka-logs"]

# Just to indicate that this container needs port map
EXPOSE 9092

ENTRYPOINT [ "kafka-server-start.sh", "/opt/kafka_2.13-3.2.1/config/server.properties" ]

CMD [ "--override", "zookeeper.connect=localhost:2181" ]