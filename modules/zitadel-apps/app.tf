data "zitadel_org" "default" {
  id = var.organisation_id
}

# s3sentinel acts as a resource server: it validates JWTs and checks the project ID
# is present in the token's `aud` claim. Callers must request the project scope when
# obtaining tokens from Zitadel.
resource "zitadel_project" "s3sentinel" {
  name   = "s3sentinel"
  org_id = data.zitadel_org.default.id

  lifecycle {
    ignore_changes = [has_project_check, project_role_assertion, project_role_check]
  }
}
