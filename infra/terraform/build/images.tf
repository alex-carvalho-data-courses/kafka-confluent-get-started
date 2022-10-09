resource "docker_image" "kafka_base" {
  name = "kafka-base"
  build {
    path = "../../docker/images/kafka-base/"
    tag  = ["kafka-base:latest", "kafka-base:3.2.1-1.0.0"]
    build_arg = {
      SCALA_VERSION : "2.13"
      KAFKA_VERSION : "3.2.1"
    }
  }
}

resource "docker_image" "kafka_zookeeper" {
  name       = "kafka-zookeeper"
  depends_on = [docker_image.kafka_base]
  build {
    path = "../../docker/images/kafka-zookeeper/"
    tag  = ["kafka-zookeeper:latest", "kafka-zookeeper:3.2.1-1.0.0"]
  }
}

resource "docker_image" "kafka_server" {
  name       = "kafka-server"
  depends_on = [docker_image.kafka_base]
  build {
    path = "../../docker/images/kafka-server/"
    tag  = ["kafka-server:latest", "kafka-server:3.2.1-1.0.0"]
  }
}