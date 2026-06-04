terraform {
  required_providers {
    helm = {
      source  = "hashicorp/helm"
      version = "3.1.2"
    }
  }
  required_version = "1.11.4"
}
