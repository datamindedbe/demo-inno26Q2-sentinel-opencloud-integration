data "zitadel_org" "default" {
  id = var.organisation_id
}

resource "zitadel_project" "trino" {
  name   = "trino"
  org_id = data.zitadel_org.default.id
}

resource "zitadel_application_oidc" "trino" {
  project_id = zitadel_project.trino.id
  org_id     = data.zitadel_org.default.id

  name                      = "trino"
  redirect_uris             = ["https://trino.${var.hosted_domain}/oauth2/callback"]
  response_types            = ["OIDC_RESPONSE_TYPE_CODE"]
  grant_types               = ["OIDC_GRANT_TYPE_AUTHORIZATION_CODE"]
  post_logout_redirect_uris = ["http://localhost"]
  dev_mode                  = true
  auth_method_type          = "OIDC_AUTH_METHOD_TYPE_BASIC"
}

# s3sentinel acts as a resource server: it validates JWTs and checks the project ID
# is present in the token's `aud` claim. Callers must request the project scope when
# obtaining tokens from Zitadel.
resource "zitadel_project" "s3sentinel" {
  name   = "s3sentinel"
  org_id = data.zitadel_org.default.id
}