variable "bundle_endpoint" {
  type        = string
  description = "S3-compatible endpoint URL serving OPA policy bundles (e.g. https://obj.upcloudobjects.com). The bucket is appended path-style."
}

variable "bundle_bucket" {
  type        = string
  description = "S3 bucket containing the OPA policy bundle."
}

variable "bundle_region" {
  type        = string
  description = "Region used for SigV4 signing against the bundle bucket."
}

variable "bundle_resource" {
  type        = string
  default     = "bundle.tar.gz"
  description = "Object key of the OPA policy bundle within the bucket."
}

variable "bundle_access_key" {
  type        = string
  sensitive   = true
  description = "Access key for reading the OPA bundle bucket."
}

variable "bundle_secret_key" {
  type        = string
  sensitive   = true
  description = "Secret key for reading the OPA bundle bucket."
}
