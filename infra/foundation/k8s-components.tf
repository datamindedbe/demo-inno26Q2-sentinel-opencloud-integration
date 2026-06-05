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

# Observability: Prometheus (scrape + store) + Grafana (historic dashboards).
# Gives retained CPU/memory time-series per pod — e.g. to watch s3sentinel
# under load. Grafana is exposed through Traefik at grafana.<domain>.
resource "helm_release" "kube_prometheus_stack" {
  name             = "kube-prometheus-stack"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "kube-prometheus-stack"
  version          = "65.1.0"
  namespace        = "monitoring"
  create_namespace = true

  values = [
    <<EOF
# Alertmanager not needed for this demo — keep the footprint small.
alertmanager:
  enabled: false

grafana:
  ingress:
    enabled: true
    ingressClassName: traefik
    annotations:
      traefik.ingress.kubernetes.io/router.entrypoints: websecure
      traefik.ingress.kubernetes.io/router.tls: "true"
      traefik.ingress.kubernetes.io/router.tls.certresolver: letsencrypt
    hosts:
      - grafana.${var.hosted_domain}
    tls:
      - secretName: grafana-tls
        hosts:
          - grafana.${var.hosted_domain}

prometheus:
  prometheusSpec:
    # Retain ~1 week of metrics on a persistent volume (default StorageClass).
    retention: 7d
    storageSpec:
      volumeClaimTemplate:
        spec:
          accessModes: ["ReadWriteOnce"]
          resources:
            requests:
              storage: 20Gi
EOF
  ]

  depends_on = [
    local_sensitive_file.kubeconfig,
    module.traefik,
  ]
}

# Dedicated Grafana dashboard for s3sentinel CPU/memory. The Grafana sidecar
# auto-imports any ConfigMap in its namespace labelled grafana_dashboard=1.
resource "kubernetes_config_map" "grafana_dashboard_s3sentinel" {
  metadata {
    name      = "grafana-dashboard-s3sentinel"
    namespace = "monitoring"
    labels = {
      grafana_dashboard = "1"
    }
  }

  data = {
    "s3sentinel.json" = file("${path.module}/dashboards/s3sentinel.json")
  }

  depends_on = [helm_release.kube_prometheus_stack]
}
