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

