variable "domain" {
  type        = string
  description = "The hosted domain for your data platform. Used to derive the proxy hostname and Zitadel endpoints."
}

variable "backend_endpoint" {
  type        = string
  description = "Full URL of the S3-compatible backend, e.g. https://yourbucket.upcloudobjects.com"
}

variable "backend_region" {
  type        = string
  description = "AWS/S3 region of the backend, e.g. europe-1."
}

variable "backend_access_key" {
  type        = string
  sensitive   = true
  description = "Access key for the S3 backend service account."
}

variable "backend_secret_key" {
  type        = string
  sensitive   = true
  description = "Secret key for the S3 backend service account."
}

variable "opa_endpoint" {
  type        = string
  default     = "http://opa.opa.svc.cluster.local:8181/v1/data/s3/allow"
  description = "OPA policy endpoint for S3 access control decisions."
}

variable "jwt_audience" {
  type        = string
  description = "Expected 'aud' claim in JWTs, typically the Zitadel project resource ID or client ID."
}

variable "sts_token_secret" {
  type        = string
  default     = ""
  sensitive   = true
  description = "HMAC secret used to sign/validate STS session tokens. When empty, the STS server is disabled and port 8090 is not exposed."
}

variable "sts_token_ttl" {
  type        = string
  default     = "1h"
  description = "Lifetime of STS-issued credentials (Go duration string, e.g. '1h', '30m')."
}
