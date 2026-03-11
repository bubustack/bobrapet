# Prerequisites

BubuStack runs on Kubernetes and depends on a small set of external components.
This page lists everything you need before installing the platform.

## Who this is for

- Platform operators deploying BubuStack for the first time.
- Developers setting up a local environment for experimentation.

## What you'll get

- The full list of system dependencies and why each is needed.
- Compatible storage backends for payload offloading.
- Minimum Kubernetes version requirements.

## Kubernetes

BubuStack targets **Kubernetes 1.30+**. Any conformant cluster works: kind, k3s,
EKS, GKE, AKS, or bare-metal kubeadm.

For local development, [kind](https://kind.sigs.k8s.io/) is recommended.

## Dependencies

### cert-manager

BubuStack uses admission webhooks for CRD validation and mutation. Webhooks
require TLS certificates. [cert-manager](https://cert-manager.io/) automates
certificate provisioning and rotation.

**Version:** v1.19+ recommended.

Install:

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.19.2/cert-manager.yaml
kubectl wait --for=condition=ready pod \
  -l app.kubernetes.io/instance=cert-manager \
  -n cert-manager --timeout=300s
```

### S3-compatible object storage

BubuStack offloads large payloads, outputs, and logs to object storage using the
S3 API. Any S3-compatible backend works. Support for additional storage APIs
(GCS, Azure Blob) is planned for future releases.

**Compatible backends:**

| Backend | Notes |
|---------|-------|
| [SeaweedFS](https://github.com/seaweedfs/seaweedfs) | Lightweight, good for local/dev. Used in the quickstart. |
| [Garage](https://garagehq.deuxfleurs.fr/) | Distributed, self-hosted, geo-aware. |
| [RustFS](https://rustfs.com/) | High-performance Rust-based S3 server. |
| [CubeFS](https://cubefs.io/) | Cloud-native distributed storage. |
| [MinIO](https://min.io/) | Popular S3-compatible server. |
| AWS S3 | Native S3. |

The quickstart uses SeaweedFS for simplicity. See `/docs/getting-started/quickstart.md`
for the install steps.

**Required configuration:**

The operator needs these settings (configured via the operator ConfigMap or Helm
values):

| Key | Description |
|-----|-------------|
| `controller.storage.s3.bucket` | Bucket name (default: `bubu-default`) |
| `controller.storage.s3.region` | S3 region (default: `us-east-1`) |
| `controller.storage.s3.endpoint` | S3 endpoint URL |
| `controller.storage.s3.use-path-style` | Use path-style addressing (required for most non-AWS backends) |
| `controller.storage.s3.auth-secret-name` | K8s Secret with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` |

See `/docs/operator/configuration.md` for the full configuration reference.

## Related docs

- `/docs/getting-started/quickstart.md` -- Install BubuStack and run your first example.
- `/docs/operator/configuration.md` -- Operator configuration keys and defaults.
- `/docs/overview/architecture.md` -- System architecture and module map.
