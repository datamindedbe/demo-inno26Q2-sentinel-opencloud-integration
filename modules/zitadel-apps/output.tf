output "trino_secret" {
  sensitive = true
  value = {
    CLIENT_ID     = zitadel_application_oidc.trino.client_id
    CLIENT_SECRET = zitadel_application_oidc.trino.client_secret
  }
}

output "s3sentinel_project_id" {
  description = "Zitadel project resource ID for s3sentinel. Used as the expected JWT 'aud' claim."
  value       = zitadel_project.s3sentinel.id
}