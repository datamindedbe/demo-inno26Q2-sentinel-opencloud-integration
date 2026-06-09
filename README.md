<!-- PROJECT SHIELDS -->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![APACHE License][license-shield]][license-url]

# S3Sentinel + UpCloud — Innovation Days 2026 Q2

> **This is a fork of the [UpCloud Data Platform](https://github.com/datamindedbe/demo-upcloud-data-platform) repo**, created for the [Dataminded](https://www.dataminded.com) Innovation Days on **June 4–5, 2026**.
>
> The goal was to integrate and benchmark [s3sentinel](https://github.com/nclaeys/s3sentinel) — an S3-compatible reverse proxy built by [Niels Claeys](https://github.com/nclaeys) that adds identity-aware, policy-driven access control on top of any S3-compatible object storage — against a real UpCloud S3 endpoint, with OIDC authentication via ZITADEL.

## What is s3sentinel?

[s3sentinel](https://github.com/nclaeys/s3sentinel) sits in front of your S3-compatible bucket, owns the service-account credentials, and authorises every S3 operation against [OPA](https://www.openpolicyagent.org/) using the caller's OIDC identity — without requiring any changes to existing S3 client code.

EU cloud providers (UpCloud, OVHcloud, Scaleway, Hetzner, …) issue bucket-level credentials with no support for STS or resource-level policies. s3sentinel solves this by adding a fine-grained authorization layer in front of the storage, supporting two auth flows:

- **Direct JWT**: client sends an OIDC token on every request
- **STS credential vending**: client exchanges a JWT for short-lived AWS credentials via `AssumeRoleWithWebIdentity`, then uses standard SigV4 — compatible with boto3, AWS CLI, Spark, DuckDB out of the box

## What we built

On top of the existing UpCloud data platform stack, we added:

- **[s3sentinel](modules/s3sentinel/)**: Deployed as a Kubernetes service. All S3 traffic is proxied through it; every request is authenticated via JWT (ZITADEL `client_credentials` grant) and authorized by OPA before being forwarded to UpCloud object storage.
- **[OPA policy engine](modules/opa/)**: IAM-style policies defined in `data.json`, evaluated by `test.rego`. Policies are structured around 30 data products, each with owned prefixes (full read/write) and granted read access to specific external prefixes. Actions are simplified to `read`/`write` in the policy data, with the mapping to S3 action names handled in Rego.
- **[S3 benchmark tool](s3-benchmark/)**: A Python benchmark that runs write-big-file, write-many-small-files, read-big-file, read-many-small-files, and delete operations in parallel across multiple pods. Results are pushed to a Prometheus Pushgateway and tagged with an `experiment_id` for comparison.
- **ZITADEL machine user auth**: Each data product has a ZITADEL service user. The benchmark uses the `client_credentials` grant to obtain a JWT, then exchanges it for scoped STS credentials via s3sentinel's `AssumeRoleWithWebIdentity` endpoint.

## Team

| Person | Contributions |
|---|---|
| [Niels Claeys](https://github.com/nclaeys) | Author of s3sentinel; architecture, Terraform/K8s integration |
| [Casper Teirlinck](https://github.com/CasperTeirlinck) | K8s deployment, OPA wiring, sentinel configuration |
| [Jeroen Bosmans](https://github.com/jeroenbosmans) | S3 benchmark tool, Grafana dashboard, K8s job manifests |
| [Wim Berchmans](https://github.com/wrrb) | ZITADEL OIDC auth flow, STS integration, OPA policy engine & Rego |
| [Jasper Goris](https://github.com/jaspergoris) | OPA policy engine contributions |
| [Gergely Soti](https://github.com/gsoti) | Portal integration |

## Benchmark findings

We ran a series of benchmarks to measure the overhead of s3sentinel on UpCloud object storage. Each benchmark pod writes one large file (2500 MB), writes 200 small files, reads them back, and deletes them. Metrics are pushed to Prometheus Pushgateway per pod, tagged with `experiment_id`.

### Setup

- **Object storage**: UpCloud S3-compatible endpoint (`fn170.upcloudobjects.com`, region `europe-1`)
- **Cluster**: Kubernetes on UpCloud
- **Auth**: ZITADEL `client_credentials` → STS `AssumeRoleWithWebIdentity` → scoped SigV4 credentials
- **Observability**: Prometheus + Grafana in `monitoring` namespace; per-pod metrics via Pushgateway

### Results

| Experiment | Pods | Via sentinel | write_big (avg) | read_big (avg) | Write MB/s | Read MB/s | Total duration |
|---|---|---|---|---|---|---|---|
| Baseline (no resource limits) | 20 | ✅ | 412 s | 275 s | 8.5 | 21.8 | 14 min |
| `exp-20260605-142202` | 5 | ✅ | **183 s** | **160 s** | **13.7** | 16.4 | 6.5 min |
| `exp-20260605-142220` | 5 | ❌ | **190 s** | **164 s** | 13.2 | 15.5 | 6.4 min |
| `exp-20260605-142900` | 20 | ❌ | 729 s | 344 s | 3.5 | 22.9 | 24.7 min |
| `exp-20260605-142928` | 20 | ✅ | 814 s | 539 s | 3.1 | 4.7 | 24.2 min |

### Key findings

1. **Resource limits matter**: The baseline run had no resource limits on the s3sentinel pod (`resources: {}`), causing CPU starvation under load. Adding proper limits cut write time from ~412 s to ~183 s (**2.2× faster**) and read time from ~275 s to ~160 s (**1.7× faster**).

2. **Sentinel overhead is negligible at low concurrency**: At 5 pods, direct S3 (`exp-142220`) and sentinel-proxied (`exp-142202`) are statistically identical — ~190 s vs ~183 s write, ~164 s vs ~160 s read. OPA policy evaluation and JWT validation add no measurable latency at this scale.

3. **Network saturation dominates at high concurrency**: At 20 parallel pods each writing a 2500 MB file, write throughput collapses from ~13.5 MB/s per pod (5 pods) to ~3.3 MB/s per pod (20 pods). The aggregate write throughput stays roughly constant at ~68 MB/s, indicating the cluster uplink to UpCloud is the hard limit.

4. **Sentinel adds read overhead under contention**: At 20 pods, sentinel read_big takes 539 s vs 344 s direct (1.6× slower). At 5 pods there was no difference. Under saturation the extra hop and per-request OPA/JWT overhead becomes visible.

5. **Sweet spot is ~5 concurrent write pods** for this cluster/uplink combination before hitting network saturation.

### Recommendations

- Keep concurrent write pods ≤ 5–6 to stay within the available uplink bandwidth (~68 MB/s aggregate)
- Always set CPU/memory resource requests and limits on s3sentinel
- For higher write parallelism, consider distributing benchmark pods across nodes with dedicated uplinks, or use multipart uploads with smaller chunk sizes

---

## Original stack

This repo is built on top of the [UpCloud Data Platform](https://github.com/datamindedbe/demo-upcloud-data-platform), which includes:

- **[Trino](https://trino.io/)**: Distributed SQL engine for interactive queries
- **[Lakekeeper](https://docs.lakekeeper.io/)**: Iceberg metadata catalog
- **[OPA](https://www.openpolicyagent.org/)**: Fine-grained access control policy engine
- **[Traefik](https://traefik.io/)**: Reverse proxy and ingress controller
- **[Zitadel](https://zitadel.com/)**: Identity and access management

See the [tutorial](docs/Tutorial.md) for deployment instructions.

## Support

Reach out to `niels.claeys@dataminded.com` or anyone at [Dataminded](https://www.dataminded.com/contact).

[contributors-shield]: https://img.shields.io/github/contributors/datamindedbe/demo-inno26Q2-sentinel-opencloud-integration.svg?style=for-the-badge
[contributors-url]: https://github.com/datamindedbe/demo-inno26Q2-sentinel-opencloud-integration/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/datamindedbe/demo-inno26Q2-sentinel-opencloud-integration.svg?style=for-the-badge
[forks-url]: https://github.com/datamindedbe/demo-inno26Q2-sentinel-opencloud-integration/network/members
[license-shield]: https://img.shields.io/github/license/datamindedbe/demo-inno26Q2-sentinel-opencloud-integration.svg?label=license&style=for-the-badge
[license-url]: https://github.com/datamindedbe/demo-inno26Q2-sentinel-opencloud-integration/blob/master/LICENSE.md