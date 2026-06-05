terraform {
  required_providers {
    zitadel = {
      source  = "zitadel/zitadel"
      version = "2.12.8"
    }
  }
}

provider "zitadel" {
  domain           = "https://zitadel.sentinel.playground.dataminded.cloud"
  insecure         = "false"
  # port             = "8080"
  jwt_profile_file = "./zitadel.json"
}

# data "zitadel_org" "default" {
#   name = "terraform-test"
# }

locals {
  products = jsondecode(file("${path.module}/new_world.json"))
  products_set = toset([for p in local.products : p.name])

  machine_users_export = {
    for k in sort(keys(zitadel_machine_user.default)) :
    k => {
      name     = zitadel_machine_user.default[k].name
      client_id     = zitadel_machine_user.default[k].client_id
      client_secret = zitadel_machine_user.default[k].client_secret
      id = zitadel_machine_user.default[k].id
    }
  }
}

resource "zitadel_machine_user" "default" {
  for_each = local.products_set

  org_id      = "375923797557051622"
  user_name   = each.key
  name        = each.key
  description = "a machine user"
  with_secret = true
  access_token_type = "ACCESS_TOKEN_TYPE_JWT"
}

output "machine_users_credentials" {
  description = "Per-product machine user credentials (client_id + client_secret)."
  value       = local.machine_users_export
  sensitive   = true
}

resource "local_file" "machine_users_credentials_json" {
  filename        = "${path.module}/machine_users_credentials.json"
  content         = jsonencode(local.machine_users_export)
  file_permission = "0600"
}