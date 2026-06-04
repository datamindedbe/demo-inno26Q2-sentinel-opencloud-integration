module "lakekeeper" {
  source = "../../modules/lakekeeper"
  domain = var.hosted_domain
}

module "trino" {
  source = "../../modules/trino"
  domain = var.hosted_domain
}

module "opa" {
  source = "../../modules/opa"
}

module "airflow" {
  source = "../../modules/airflow"
  domain = var.hosted_domain
}

module "s3sentinel" {
  source = "../../modules/s3sentinel"

  domain             = var.hosted_domain
  backend_endpoint   = "https://${var.storage_bucket_domain_name}"
  backend_region     = var.region
  backend_access_key = var.s3sentinel_backend_access_key
  backend_secret_key = var.s3sentinel_backend_secret_key
  jwt_audience       = module.zitadel_apps.s3sentinel_project_id
}