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

# Prometheus Pushgateway: the s3-benchmark Jobs are short-lived, so Prometheus
# can't scrape them directly. Each pod pushes its per-operation results here on
# completion; Prometheus scrapes the gateway and retains the series.
#
# - fullnameOverride keeps the Service name stable at `pushgateway` so the Job's
#   PUSHGATEWAY_URL (pushgateway.monitoring.svc.cluster.local:9091) is fixed.
# - serviceMonitor.additionalLabels.release matches kube-prometheus-stack's
#   ServiceMonitor selector, so the operator actually scrapes it.
# - honorLabels keeps the labels the pods push (run_id, identity, instance)
#   instead of overwriting them with the gateway's own target labels.
resource "helm_release" "prometheus_pushgateway" {
  name             = "pushgateway"
  repository       = "https://prometheus-community.github.io/helm-charts"
  chart            = "prometheus-pushgateway"
  version          = "2.15.0"
  namespace        = "monitoring"
  create_namespace = false

  values = [
    <<EOF
fullnameOverride: pushgateway

serviceMonitor:
  enabled: true
  namespace: monitoring
  honorLabels: true
  additionalLabels:
    release: kube-prometheus-stack
EOF
  ]

  depends_on = [helm_release.kube_prometheus_stack]
}

# Grafana dashboard aggregating the s3-benchmark results across all parallel
# pods (throughput, rate, wall time, and per-identity success/access-denied).
resource "kubernetes_config_map" "grafana_dashboard_s3_benchmark" {
  metadata {
    name      = "grafana-dashboard-s3-benchmark"
    namespace = "monitoring"
    labels = {
      grafana_dashboard = "1"
    }
  }

  data = {
    "s3-benchmark.json" = file("${path.module}/dashboards/s3-benchmark.json")
  }

  depends_on = [helm_release.kube_prometheus_stack]
}
