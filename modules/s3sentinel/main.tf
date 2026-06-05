resource "helm_release" "s3sentinel" {
  name      = "s3sentinel"
  chart     = "${path.module}/chart"
  namespace = "services"

  values = [
    yamlencode(merge(
      {
        proxyHost   = "s3sentinel.${var.domain}"
        opaEndpoint = var.opa_endpoint

        backend = {
          endpoint  = var.backend_endpoint
          region    = var.backend_region
          accessKey = var.backend_access_key
          secretKey = var.backend_secret_key
        }

        jwt = {
          jwksEndpoint = "https://zitadel.${var.domain}/oauth/v2/keys"
          issuer       = "https://zitadel.${var.domain}"
          audience     = var.jwt_audience
        }

        ingress = {
          enabled   = true
          className = "traefik"
          annotations = {
            "traefik.ingress.kubernetes.io/router.entrypoints"      = "websecure"
            "traefik.ingress.kubernetes.io/router.tls.certresolver" = "letsencrypt"
          }
          hosts = [
            {
              host  = "s3sentinel.${var.domain}"
              paths = [{ path = "/", pathType = "Prefix" }]
            }
          ]
          tls = [
            {
              secretName = "s3sentinel-tls"
              hosts      = ["s3sentinel.${var.domain}"]
            }
          ]
        }
      },
      var.sts_token_secret != "" ? {
        sts = {
          tokenSecret = var.sts_token_secret
          tokenTTL    = var.sts_token_ttl
        }
      } : {}
    ))
  ]
}
