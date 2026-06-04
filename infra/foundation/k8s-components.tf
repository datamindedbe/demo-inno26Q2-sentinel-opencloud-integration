module "traefik" {
  source     = "../../modules/traefik"
  domain     = var.hosted_domain
  depends_on = [kubernetes_namespace.traefik]
}
module "zitadel" {
  source              = "../../modules/zitadel"
  domain              = var.hosted_domain
  zitadel_admin_email = var.admin_email
  depends_on          = [kubernetes_namespace.services]
}
