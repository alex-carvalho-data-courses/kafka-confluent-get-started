terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "2.21.0"
    }
  }

  required_version = ">= 0.13"
}

provider "docker" {
  host = "unix:///var/run/docker.sock"
}

#####################################################
# network
#####################################################
resource "docker_network" "kafka_network" {
  name = "kafka-network"
}

#####################################################
# containers
#####################################################
resource "docker_container" "kafka_zookeeper" {
  name  = "kafka-zookeeper"
  image = docker_image.kafka_zookeeper.image_id

  networks_advanced {
    name = docker_network.kafka_network.id
  }

  volumes {
    volume_name    = "kafka-zookeeper-config-vol"
    container_path = "/opt/kafka_2.13-3.2.1/config"
  }

  volumes {
    volume_name    = "kafka-zookeeper-data-vol"
    container_path = "/tmp/zookeeper"
  }

  healthcheck {
    test         = ["CMD", "echo", "ruok", "|", "nc", "localhost", "2181", "|", "grep", "imok"]
    interval     = "10s"
    retries      = 10
    start_period = "3s"
    timeout      = "10s"
  }
}

resource "docker_container" "kafka_broker_101" {
  name  = "kafka-broker-101"
  image = docker_image.kafka_server.image_id

  depends_on = [docker_container.kafka_zookeeper]

  networks_advanced {
    name = docker_network.kafka_network.id
  }

  volumes {
    volume_name    = "kafka-broker-config-vol"
    container_path = "/opt/kafka_2.13-3.2.1/config"
  }

  volumes {
    volume_name    = "kafka-broker-101-logs-vol"
    container_path = "/tmp/kafka-logs"
  }

  ports {
    external = "19092"
    internal = "19092"
  }

  command = [
    "--override", "zookeeper.connect=${docker_container.kafka_zookeeper.name}:2181",
    "--override", "broker.id=101",
    "--override", "listeners=PLAINTEXT://:9092,CONNECTIONS_FROM_HOST://:19092",
    "--override", "advertised.listeners=PLAINTEXT://kafka-broker-101:9092,CONNECTIONS_FROM_HOST://localhost:19092",
    "--override", "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONNECTIONS_FROM_HOST:PLAINTEXT"
  ]

  restart = "on-failure"

  healthcheck {
    test         = ["CMD", "nc", "-z", "kafka-broker-101", "9092"]
    interval     = "3s"
    retries      = 10
    start_period = "3s"
    timeout      = "10s"
  }
}


resource "docker_container" "kafka_broker_102" {
  name  = "kafka-broker-102"
  image = docker_image.kafka_server.image_id

  depends_on = [docker_container.kafka_zookeeper]

  networks_advanced {
    name = docker_network.kafka_network.id
  }

  volumes {
    volume_name    = "kafka-broker-config-vol"
    container_path = "/opt/kafka_2.13-3.2.1/config"
  }

  volumes {
    volume_name    = "kafka-broker-102-logs-vol"
    container_path = "/tmp/kafka-logs"
  }

  ports {
    external = "29092"
    internal = "29092"
  }

  command = [
    "--override", "zookeeper.connect=${docker_container.kafka_zookeeper.name}:2181",
    "--override", "broker.id=102",
    "--override", "listeners=PLAINTEXT://:9092,CONNECTIONS_FROM_HOST://:29092",
    "--override", "advertised.listeners=PLAINTEXT://kafka-broker-102:9092,CONNECTIONS_FROM_HOST://localhost:29092",
    "--override", "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONNECTIONS_FROM_HOST:PLAINTEXT"
  ]

  restart = "on-failure"

  healthcheck {
    test         = ["CMD", "nc", "-z", "kafka-broker-102", "9092"]
    interval     = "3s"
    retries      = 10
    start_period = "3s"
    timeout      = "10s"
  }
}

resource "docker_container" "kafka_broker_103" {
  name  = "kafka-broker-103"
  image = docker_image.kafka_server.image_id

  depends_on = [docker_container.kafka_zookeeper]

  networks_advanced {
    name = docker_network.kafka_network.id
  }

  volumes {
    volume_name    = "kafka-broker-config-vol"
    container_path = "/opt/kafka_2.13-3.2.1/config"
  }

  volumes {
    volume_name    = "kafka-broker-103-logs-vol"
    container_path = "/tmp/kafka-logs"
  }

  ports {
    external = "39092"
    internal = "39092"
  }

  command = [
    "--override", "zookeeper.connect=${docker_container.kafka_zookeeper.name}:2181",
    "--override", "broker.id=103",
    "--override", "listeners=PLAINTEXT://:9092,CONNECTIONS_FROM_HOST://:39092",
    "--override", "advertised.listeners=PLAINTEXT://kafka-broker-103:9092,CONNECTIONS_FROM_HOST://localhost:39092",
    "--override", "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONNECTIONS_FROM_HOST:PLAINTEXT"
  ]

  restart = "on-failure"

  healthcheck {
    test         = ["CMD", "nc", "-z", "kafka-broker-103", "9092"]
    interval     = "3s"
    retries      = 10
    start_period = "3s"
    timeout      = "10s"
  }
}