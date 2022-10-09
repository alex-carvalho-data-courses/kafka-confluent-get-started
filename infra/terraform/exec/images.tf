resource "docker_image" "kafka_zookeeper" {
  name         = "kafka-zookeeper:latest"
  keep_locally = true
}

resource "docker_image" "kafka_server" {
  name         = "kafka-server:latest"
  keep_locally = true
}