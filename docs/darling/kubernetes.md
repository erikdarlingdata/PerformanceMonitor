# Run Darling on Kubernetes

Audience: operators.

The repository carries a Helm chart for Darling: [`Darling/helm/performancemonitor-darling`](../../Darling/helm/performancemonitor-darling/README.md). It runs the service as one pod with a state volume, with either a bundled TimescaleDB pod or a connection to a PostgreSQL server you run. The chart is not published to a registry yet, so you install it from a checkout:

```
helm install darling Darling/helm/performancemonitor-darling --set-string image.tag=<release>
```

Pin `image.tag` to a release, because `latest` moves. Use `--set-string` with `--set`, because a float-looking tag such as `3.10` loses digits otherwise.

The chart README is the reference. It covers:

- [the two store modes](../../Darling/helm/performancemonitor-darling/README.md#the-two-store-modes), and why custom alerts need a managed or compose store for now;
- [which values the store overrides after the first start](../../Darling/helm/performancemonitor-darling/README.md#which-values-the-store-overrides-after-the-first-start) (see also [`darling.json` is only a seed](install.md#darlingjson-is-only-a-seed));
- [secrets, and why a GitOps install must name its own Secrets](../../Darling/helm/performancemonitor-darling/README.md#secrets-and-gitops);
- [the password key](../../Darling/helm/performancemonitor-darling/README.md#the-password-key) (see also [Keeping the key outside the service](security.md#security--least-privilege-roles));
- [readiness](../../Darling/helm/performancemonitor-darling/README.md#readiness), which follows the collector (see [the health port](network-endpoints.md#the-health-port));
- [upgrades](../../Darling/helm/performancemonitor-darling/README.md#upgrades).

Out of the box only `kubectl port-forward` reaches the dashboard. An Ingress needs `allowFrom` per surface (the dashboard judges the client address the controller forwards, the MCP endpoint the controller's own connection) and, for the dashboard, `trustedProxies`; see [reaching it from outside the cluster](../../Darling/helm/performancemonitor-darling/README.md#reaching-it-from-outside-the-cluster) and [Opt-in Network Endpoints](network-endpoints.md).

If you write the manifests yourself instead, the pod needs the `securityContext` and the volume ownership steps in [Container user](security.md#container-user).
