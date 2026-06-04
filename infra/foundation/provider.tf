terraform {
  required_providers {
    upcloud = {
      source  = "UpCloudLtd/upcloud"
      version = "5.38.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "2.38.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "3.1.2"
    }
    random = {
      source  = "hashicorp/random"
      version = "3.6.3"
    }
    local = {
      source  = "hashicorp/local"
      version = "2.5.3"
    }
  }
}

provider "upcloud" {
}

provider "kubernetes" {
  config_path = local_sensitive_file.kubeconfig.filename
}

provider "helm" {
  kubernetes = {
    config_path = local_sensitive_file.kubeconfig.filename
  }
}
