# Run Darling on Kubernetes

Audience: operators.

The repository carries a Helm chart for Darling: [`Darling/helm/performancemonitor-darling`](../../Darling/helm/performancemonitor-darling/README.md). It runs the service as one pod with a state volume, with either a bundled TimescaleDB pod or a connection to a PostgreSQL server you run.

Each release publishes the chart, starting with the first release that includes the publishing workflow. Install it by version:

```
helm install darling oci://ghcr.io/erikdarlingdata/charts/performancemonitor-darling --version <release>
```

The published chart pins its release's image by tag and digest, so it needs no `image.tag`. The release's assets also carry the chart as `performancemonitor-darling-<release>.tgz` with `SHA256SUMS-helm.txt`, for installs without registry access (see the chart README's [Offline install](../../Darling/helm/performancemonitor-darling/README.md#offline-install)).

To install a commit that is not released yet, use a checkout:

```
helm install darling Darling/helm/performancemonitor-darling --set-string image.tag=<release>
```

Pin `image.tag` to a release, because `latest` moves. Use `--set-string`, not `--set`, because a float-looking tag such as `3.10` loses digits otherwise.

The chart README is the reference. It covers:

- [the two store modes](../../Darling/helm/performancemonitor-darling/README.md#the-two-store-modes), and why custom alerts need a managed or compose store for now;
- [which values the store overrides after the first start](../../Darling/helm/performancemonitor-darling/README.md#which-values-the-store-overrides-after-the-first-start) (see also [`darling.json` is only a seed](install.md#darlingjson-is-only-a-seed));
- [secrets, and why a GitOps install must name its own Secrets](../../Darling/helm/performancemonitor-darling/README.md#secrets-and-gitops);
- [the password key](../../Darling/helm/performancemonitor-darling/README.md#the-password-key) (see also [Keeping the key outside the service](security.md#security--least-privilege-roles));
- [readiness](../../Darling/helm/performancemonitor-darling/README.md#readiness), which follows the collector (see [the health port](network-endpoints.md#the-health-port));
- [upgrades](../../Darling/helm/performancemonitor-darling/README.md#upgrades);
- [uninstall](../../Darling/helm/performancemonitor-darling/README.md#uninstall), which keeps the volumes and the Secrets.

Out of the box only `kubectl port-forward` reaches the dashboard. An Ingress needs `allowFrom` per surface (the dashboard judges the client address the controller forwards, the MCP endpoint the controller's own connection) and, for the dashboard, `trustedProxies`; see [reaching it from outside the cluster](../../Darling/helm/performancemonitor-darling/README.md#reaching-it-from-outside-the-cluster) and [Opt-in Network Endpoints](network-endpoints.md).

If you write the manifests yourself instead, the pod needs the `securityContext` and the volume ownership steps in [Container user](security.md#container-user). Moving from your own manifests to the chart later is covered in the chart README's [Moving from your own manifests](../../Darling/helm/performancemonitor-darling/README.md#moving-from-your-own-manifests).

## Verify the chart

The published chart is signed the same way as the image: a keyless Sigstore signature made by this repository's release workflow. Check it before installing:

```
cosign verify ghcr.io/erikdarlingdata/charts/performancemonitor-darling:<release> \
  --certificate-identity-regexp 'github.com/erikdarlingdata/PerformanceMonitor' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

For the `.tgz` from the release's assets, check it against the checksum file next to it:

```
sha256sum -c SHA256SUMS-helm.txt
```
