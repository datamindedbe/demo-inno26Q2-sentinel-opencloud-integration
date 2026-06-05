resource "kubernetes_config_map" "opa_policies" {
  metadata {
    name      = "opa-policies"
    namespace = "opa"
  }
  data = {
    for f in fileset("${path.module}/policies", "*.{rego,json}") :
    f => file("${path.module}/policies/${f}")
  }
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
      policyDirectory = "/var/lib/opa/policies"
      useHttps        = false

      image = {
        repository = "openpolicyagent/opa"
        tag        = "1.16.0"
        pullPolicy = "IfNotPresent"
      }

      extraArgs = [
        "--set=decision_logs.console=true",
        "--ignore=..*",
      ]

      extraEnv = [
        { name = "TRINO_LAKEKEEPER_CATALOG_NAME", value = "iceberg" },
        { name = "LAKEKEEPER_LAKEKEEPER_WAREHOUSE", value = "iceberg" },
      ]

      extraVolumes = [{
        name = "policies"
        configMap = {
          name = kubernetes_config_map.opa_policies.metadata[0].name
        }
      }]

      extraVolumeMounts = [{
        name      = "policies"
        mountPath = "/var/lib/opa/policies"
        readOnly  = true
      }]

      authz = { enabled = false }
      mgmt  = { enabled = false }
    })
  ]

  depends_on = [kubernetes_config_map.opa_policies]
}
