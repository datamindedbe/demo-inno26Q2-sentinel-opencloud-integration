resource "kubernetes_secret" "opa_bundle_credentials" {
  metadata {
    name      = "opa-bundle-credentials"
    namespace = "opa"
  }

  data = {
    AWS_ACCESS_KEY_ID     = var.bundle_access_key
    AWS_SECRET_ACCESS_KEY = var.bundle_secret_key
    AWS_REGION            = var.bundle_region
  }

  type = "Opaque"
}

resource "helm_release" "opa" {
  chart       = "${path.module}/helm/opa-kube-mgmt"
  name        = "opa"
  namespace   = "opa"
  max_history = 10
  values = [
    yamlencode({
      logLevel        = "debug"
      logFormat       = "json"
      useHttps        = false
      policyDirectory = ""

      image = {
        repository = "openpolicyagent/opa"
        tag        = "1.16.0"
        pullPolicy = "IfNotPresent"
      }

      extraEnv = [
        { name = "TRINO_LAKEKEEPER_CATALOG_NAME", value = "iceberg" },
        { name = "LAKEKEEPER_LAKEKEEPER_WAREHOUSE", value = "iceberg" },
        {
          name = "AWS_ACCESS_KEY_ID"
          valueFrom = {
            secretKeyRef = {
              name = kubernetes_secret.opa_bundle_credentials.metadata[0].name
              key  = "AWS_ACCESS_KEY_ID"
            }
          }
        },
        {
          name = "AWS_SECRET_ACCESS_KEY"
          valueFrom = {
            secretKeyRef = {
              name = kubernetes_secret.opa_bundle_credentials.metadata[0].name
              key  = "AWS_SECRET_ACCESS_KEY"
            }
          }
        },
        {
          name = "AWS_REGION"
          valueFrom = {
            secretKeyRef = {
              name = kubernetes_secret.opa_bundle_credentials.metadata[0].name
              key  = "AWS_REGION"
            }
          }
        },
      ]

      opa = {
        services = {
          policies = {
            url = "${var.bundle_endpoint}/${var.bundle_bucket}"
            credentials = {
              s3_signing = {
                environment_credentials = {}
              }
            }
          }
        }
        bundles = {
          authz = {
            service  = "policies"
            resource = var.bundle_resource
            polling = {
              min_delay_seconds = 5
              max_delay_seconds = 5
            }
          }
        }
        decision_logs = {
          console = true
        }
      }

      authz = { enabled = false }
      mgmt  = { enabled = false }
    })
  ]

  depends_on = [kubernetes_secret.opa_bundle_credentials]
}
