resource "helm_release" "s3sentinel" {
  name      = "s3sentinel"
  chart     = "${path.module}/chart"
  namespace = "services"

  values = [
    yamlencode(merge(
      {
        proxyHost   = "s3sentinel.${var.domain}"
        opaEndpoint = var.opa_endpoint

        resources = {
          requests = {
            cpu    = "1"
            memory = "1Gi"
          }
          limits = {
            cpu    = "1"
            memory = "1Gi"
          }
        }

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
          hosts = concat(
            [
              {
                host  = "s3sentinel.${var.domain}"
                paths = [{ path = "/", pathType = "Prefix" }]
              }
            ],
            var.sts_token_secret != "" ? [
              {
                host        = "s3sentinel-sts.${var.domain}"
                servicePort = "sts"
                paths       = [{ path = "/", pathType = "Prefix" }]
              }
            ] : []
          )
          tls = concat(
            [
              {
                secretName = "s3sentinel-tls"
                hosts      = ["s3sentinel.${var.domain}"]
              }
            ],
            var.sts_token_secret != "" ? [
              {
                secretName = "s3sentinel-sts-tls"
                hosts      = ["s3sentinel-sts.${var.domain}"]
              }
            ] : []
          )
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
