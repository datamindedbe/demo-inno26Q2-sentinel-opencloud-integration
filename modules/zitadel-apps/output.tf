output "s3sentinel_project_id" {
  description = "Zitadel project resource ID for s3sentinel. Used as the expected JWT 'aud' claim."
  value       = zitadel_project.s3sentinel.id
}