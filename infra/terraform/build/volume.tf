resource "docker_volume" "kafka_zookeeper_config_vol" {
  name = "kafka-zookeeper-config-vol"
}

resource "docker_volume" "kafka_zookeeper_data_vol" {
  name = "kafka-zookeeper-data-vol"
}

resource "docker_volume" "kafka_broker_config_vol" {
  name = "kafka-broker-config-vol"
}

resource "docker_volume" "kafka_broker_101_logs_vol" {
  name = "kafka-broker-101-logs-vol"
}

resource "docker_volume" "kafka_broker_102_logs_vol" {
  name = "kafka-broker-102-logs-vol"
}

resource "docker_volume" "kafka_broker_103_logs_vol" {
  name = "kafka-broker-103-logs-vol"
}