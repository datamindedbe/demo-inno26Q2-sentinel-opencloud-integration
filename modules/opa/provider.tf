terraform {
  required_providers {
    helm = {
      source  = "hashicorp/helm"
      version = "3.1.2"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "2.38.0"
    }
  }
  required_version = "1.11.4"
}
