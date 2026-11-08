# performancemonitor-darling

A Helm chart for Darling, the collector service of PerformanceMonitor. It runs the service as one pod with a state volume, a Service for the web dashboard (and, when you turn it on, the MCP endpoint), optional Ingresses, and either a single bundled TimescaleDB pod or a connection to a PostgreSQL server you run.

Requires Kubernetes 1.27 or later and Helm 3. The chart is not published to a registry yet, so you install it from a checkout of this repository.

Every value is documented in [`values.yaml`](values.yaml) and checked by [`values.schema.json`](values.schema.json). This page covers what the values do together.

## Install from a checkout

```
helm install darling Darling/helm/performancemonitor-darling --set-string image.tag=<release>
```

Pin the tag. The chart's default is its `appVersion`, which is `latest`, and `latest` moves with every release: an upgrade or a pod restart could then change the collector by itself. Install notes warn when the tag is `latest`. `image.digest` pins by digest as well.

With `--set`, write `--set-string image.tag=3.13.0`. Helm parses a float-looking value as a number before the chart sees it, and a number loses digits (`3.10` becomes `3.1`). In a values file, quote the tag.

Out of the box this installs the bundled store and reaches nothing from outside the cluster. The dashboard and MCP endpoint admit only loopback, so use a port-forward:

```
kubectl port-forward deployment/<fullname> 5153:5153
```

(`<fullname>` is `<release>-performancemonitor-darling`, or just the release name when it already contains `performancemonitor-darling`; `helm install` prints it. The dashboard's Service is `<fullname>-web`.) Open `http://localhost:5153` and sign in at `/auth/login` with the web token, which the install notes show how to read. The dashboard's `/api/ping` takes the session cookie from that sign-in, not a bearer header.

## The two store modes

`store.mode` is `bundled` (the default) or `external`.

**Bundled** runs one TimescaleDB pod (a StatefulSet) with the release. It suits evaluation and small installs: one replica, no backups, no failover. The image is `timescale/timescaledb:2.31.0-pg17`, the image CI tests Darling against, with the same server settings as the compose file's store. Darling's connection string carries no password. The password reaches the Darling pod as `PGPASSWORD` from the `password` key of the store Secret, the same key the store pod reads. The chart generates that Secret (`<fullname>-store`) unless you name your own in `store.bundled.auth.existingSecret`.

**External** connects to a PostgreSQL or TimescaleDB server you run, and the chart renders no store resources. `host`, `database` and `username` are required (they may not contain `;` or `=`). Pick one way to give the password:

- `passwordSecret`: a Secret you manage. The value reaches the Darling container as `PGPASSWORD`, so it never appears in `darling.json`. Leave `name` empty for a server that needs no password.
- `connectionStringSecret`: a Secret holding the whole Npgsql connection string. The chart mounts it read-only and points `postgres.connectionString` at it as a `file:` reference. `host`, `port`, `database`, `username` and `sslMode` are then ignored.
- `caSecret` (with `passwordSecret`): the CA certificate that signs the server's certificate. The chart mounts it and adds `Root Certificate` to the connection string. Pair it with `sslMode: VerifyCA` or `VerifyFull`.

A managed cloud PostgreSQL service:

```yaml
store:
  mode: external
  external:
    host: darling-db.example.com
    database: darling
    username: darling
    sslMode: VerifyFull
    passwordSecret:
      name: darling-db
      key: password
    caSecret:
      name: darling-db-ca
      key: ca.crt
```

A CloudNativePG cluster named `darling-db` in the same namespace. The operator creates the `darling-db-app` Secret (the owner's `password`) and the `darling-db-ca` Secret (`ca.crt`), and a read-write Service `darling-db-rw`:

```yaml
store:
  mode: external
  external:
    host: darling-db-rw
    database: app
    username: app
    sslMode: VerifyFull
    passwordSecret:
      name: darling-db-app
      key: password
    caSecret:
      name: darling-db-ca
      key: ca.crt
```

The Secrets must be in the release's namespace. With `VerifyFull`, the certificate must name the host you give (here, the Service name or its full DNS form).

Another worked example, with an external store, two Ingresses, `configSubstitutions` and the optional pod settings, is [`ci/external-cnpg-style.yaml`](ci/external-cnpg-style.yaml), the file CI renders.

### Custom alerts need a managed or compose store for now

The bundled store runs custom alerts. An external store does not, by design, and the chart has no store-roles Job. If you need custom alerts today, use the bundled store, or a managed or compose store ([Security](../../../docs/darling/security.md#security--least-privilege-roles) describes the roles).

## Values

| Value | Default | What it does |
|---|---|---|
| `image.repository`, `image.tag`, `image.digest`, `image.pullPolicy` | `ghcr.io/erikdarlingdata/performancemonitor-darling`, the chart's `appVersion` (`latest`), none, `IfNotPresent` | The image. Pin the tag. |
| `imagePullSecrets` | `[]` | Pull Secrets for a private registry. |
| `nameOverride`, `fullnameOverride` | empty | Resource naming. |
| `resources` | requests 250m CPU and 512Mi, limit 4Gi memory | The Darling container. |
| `persistence.size`, `.storageClass`, `.existingClaim` | `1Gi`, cluster default, none | The state volume at `/var/lib/darling`. An existing claim must be writable by user 1654. |
| `podAnnotations`, `podLabels`, `nodeSelector`, `tolerations`, `affinity`, `priorityClassName`, `hostAliases` | empty | Pod placement and metadata. |
| `extraEnv`, `extraEnvFrom`, `extraVolumes`, `extraVolumeMounts` | empty | Extras for the Darling container. |
| `config` | `{}` | Your `darling.json`: servers, collection, alerts. The chart owns some keys (below). |
| `configSubstitutions` | `[]` | `{ placeholder, secretKey }` pairs: secrets written into `darling.json` when the pod starts. |
| `passwordKey.existingSecret`, `.secretKey`, `.source` | none, `password-key`, `file` | Keep the key that seals stored server passwords in a Secret. |
| `secrets.existingSecret`, `.values`, `.webTokenKey`, `.mcpTokenKey` | none, `{}`, `web-token`, `mcp-token` | The chart Secret: tokens and your own keys. |
| `store.mode` | `bundled` | `bundled` or `external`. |
| `store.bundled.image`, `.persistence`, `.auth.existingSecret`, `.resources` | the image above, `10Gi`, none, requests 2Gi and limit 4Gi memory | The bundled store. |
| `store.external.*` | empty | The external store (above). |
| `network.allowFrom` | `[]` | Client CIDRs the dashboard and MCP endpoint admit, unless a surface sets its own. Empty renders `127.0.0.1/32`. |
| `network.trustedProxies` | `[]` | Proxy CIDRs the dashboard trusts `X-Forwarded-*` from. |
| `web.enabled`, `web.port`, `web.publicBaseUrl` | `true`, `5153`, derived | The dashboard. |
| `web.network.*`, `web.ingress.*` | empty, disabled | Per-surface overrides, and the dashboard Ingress. |
| `mcp.enabled`, `mcp.port` | `false`, `5152` | The MCP endpoint. |
| `mcp.network.allowFrom`, `mcp.ingress.*` | empty, disabled | Override, and the MCP Ingress. |

Fixed, not values: one replica, the `Recreate` strategy, and the pod security context (`runAsNonRoot`, user, group and `fsGroup` 1654, `fsGroupChangePolicy: OnRootMismatch`, `RuntimeDefault` seccomp). Every container drops all capabilities, forbids privilege escalation and has a read-only root filesystem. No container ever runs as root. The init container that writes `darling.json` has small fixed resources (requests 10m CPU and 16Mi, limit 64Mi), so a namespace with a ResourceQuota accepts the pod.

### Keys the chart owns in `config`

The chart owns exactly these keys of `darling.json`:

- `postgres.managed` and `postgres.connectionString` (from `store.*`);
- `web.enabled`, `web.port`, `web.publicBaseUrl`, `web.network.listen`, `web.network.allowFrom`, `web.network.trustedProxies` and `web.network.token`;
- `mcp.enabled`, `mcp.port`, `mcp.network.listen`, `mcp.network.allowFrom` and `mcp.network.token`;
- `health` and `passwordKey`.

Setting one inside `config` fails the render and names the value to use instead (or says the chart sets it and nothing overrides it); the chart never overrides one silently. Keys are matched ignoring case, as Darling reads them, and `config` must spell the top-level `postgres`, `web` and `mcp` exactly so: a `Postgres` block would sit beside the chart's and Darling would read only one of the two, so the render fails. Every other key under `postgres`, `web.network` and `mcp.network` is yours and is passed through, for example `postgres.webConnectionString`, `web.network.oidc`, `web.network.tls` and `mcp.network.tls`. The chart sets `mcp.network.hostName` only when `config` does not (see below).

### Through `config`

Some settings have no chart value; they go in `config`, next to the keys the chart owns.

- **OIDC sign-in for the dashboard:** `config.web.network.oidc` (`authority`, `clientId`, `clientSecret`). Give the client secret as a `file:` reference into the chart Secret, for example `clientSecret: file:/run/secrets/darling/oidc-client-secret` with the key `oidc-client-secret` in `secrets.values` or in your `existingSecret`. Behind an Ingress the dashboard needs `trustedProxies` for the `https` redirect.
- **TLS on the pod itself** (instead of, or behind, a TLS-terminating Ingress): mount the certificate with `extraVolumes` and `extraVolumeMounts`, and set `config.web.network.tls` or `config.mcp.network.tls` (`certPath` and `keyPath`, or `pfxPath` and `pfxPassword`) to the mounted paths. The Ingress then has to speak HTTPS to the pod (an annotation of your controller).
- **Least-privilege store logins:** `postgres.webConnectionString` and `postgres.mcpConnectionString` are not owned. On an external store, run `tools/provision-roles.sql` (see [Security](../../../docs/darling/security.md#security--least-privilege-roles)), put the two connection strings in the chart Secret, and point the keys at them with `file:/run/secrets/darling/<key>`.

Two warnings in the log are expected with this chart:

- On an external store without those two strings, the dashboard and the MCP endpoint each log that they connect to the store "as the owner login (postgres.connectionString) because postgres.webConnectionString is not set" (or `mcpConnectionString`). That is the documented fallback, not a fault: they work, with the owner's rights. The strings above remove it.
- Without `tls` in `config`, the dashboard and the MCP endpoint each log at every start that they serve plain HTTP and that `allowFrom` bounds only who can route to the port. Behind a TLS-terminating Ingress that is the expected shape: the plain hop is the one between the controller and the pod.

### Reaching it from outside the cluster

Darling listens on `0.0.0.0` inside the pod, and `allowFrom` decides who it serves. The default is `["127.0.0.1/32"]` (Darling refuses an empty list), so only a port-forward reaches it. For an Ingress:

- For the dashboard, `web.network.trustedProxies` (or `network.trustedProxies`) is required: the CIDRs of the ingress controller pods. The dashboard then takes the client address from the controller's `X-Forwarded-For` (the address `allowFrom` and the sign-in throttle judge) and the scheme from `X-Forwarded-Proto` (the `Secure` session cookie, and an `https` OIDC redirect).
- `allowFrom` is required, and the two surfaces judge different addresses, so set it per surface. `web.network.allowFrom` is judged on the forwarded client: list your operators' addresses, not the controller. `mcp.network.allowFrom` is judged on the connection, because the MCP endpoint never reads `X-Forwarded-For`: list the ingress controller pods' CIDR. The shared `network.allowFrom` applies to a surface that sets none.
- What the controller writes into `X-Forwarded-For` depends on how traffic reaches it: behind a LoadBalancer Service with `externalTrafficPolicy: Cluster` it is often a node address, not the operator's. Check the controller's own log before you write the web list.
- Rendering fails with a message naming the missing value otherwise.
- `web.publicBaseUrl` defaults to `https://<first web ingress host>` when a TLS entry covers it, and `http://` when none does.
- For the dashboard, Darling's Host guard admits one DNS name: the host of `web.publicBaseUrl`. With several web Ingress hosts, only that one works; the others get `400`.
- For MCP, Darling's Host guard admits one DNS name. The chart sets `mcp.network.hostName` to the first MCP Ingress host (set it in `config` to use another), so only that host works.
- A token you supply (in `secrets.values` or your `existingSecret`) must be at least 16 characters with at least 6 different characters. A shorter or more repetitive one keeps that surface loopback-only, with a Critical line in the log. The chart's generated tokens are 40 random characters.
- `trustedProxies` set to the controller pods' CIDR trusts every pod in that CIDR: any of them can claim any client address in `X-Forwarded-For`. The health port, 5154, answers anyone who can route to the pod. A NetworkPolicy that admits only the ingress controller to the web and MCP ports, and nothing else to the pod, closes both (the kubelet's probes come from the node, which most network plugins always admit).
- IPv4 is assumed: the chart sets `listen` to `0.0.0.0`. IPv6-only clusters are not supported in this version.

## Which values the store overrides after the first start

`darling.json` is only a seed for some keys. On first start the service copies them into its store; from then on the store holds the only copy, and changing the value in the chart does nothing. The rest are read from the file at every start, and a chart change reaches them through the ConfigMap, which restarts the pod.

| Keys | Where they live | How to change them after the first start |
|---|---|---|
| `alerts`, `analysis` (`enabled`, `intervalMinutes`, `notificationsEnabled`, `notifySeverity`) | First-run seed | The Viewer's Settings, or the MCP tool `update_alert_settings` |
| `smtp`, `webhooks` (including a webhook set through `configSubstitutions`) | First-run seed | The Viewer's Settings > Notifications, or the MCP tool `update_notification_channels`. Rotating the Secret behind a webhook placeholder does not change a webhook already stored |
| `capturePlans`, `queryStoreBackfillEnabled`, `queryStoreTextBudgetMb`, `queryStoreFetchTopN`, `maxConcurrentSweeps` | First-run seed | The Viewer's Settings, or the MCP tool `update_service_flags` |
| `planContentRetentionDays`, `composeStatementTimeoutSeconds`, `planXmlCompression` | First-run seed | The column in `config.config_service` in the store |
| `servers` | First-run seed, copied once while the server registry is empty. A server added to the file later is ignored | The Viewer, the dashboard's Admin page, or the MCP tools `add_servers` and `edit_server` |
| `mcp.enabled`, `mcp.port`, `web.enabled`, `web.port` (the chart's `mcp.enabled`, `mcp.port`, `web.enabled`, `web.port`) | First-run seed | The Viewer's Settings, or `--enable-mcp` / `--disable-mcp` / `--enable-web` / `--disable-web` run in the pod (`kubectl exec`). Live, no restart. A port changed this way no longer matches the chart's Service and probes, so change the chart value too |
| `postgres` (the chart's `store.*`), `passwordKey`, `health`, `mcp.network` and `web.network` (the chart's `network.*`, `web.*`, `mcp.*`), and every other key of `config` not listed above | File only | Change the chart value and upgrade |

The source is [`darling.json` is only a seed](../../../docs/darling/install.md#darlingjson-is-only-a-seed) and the [source-of-truth table](../../../docs/darling/configuration.md#where-each-setting-lives-first-run-seed-or-file-only) in the configuration reference, which has the full list. So `servers`, `alerts` and `webhooks` in `config` are right for the first install and for a store you are rebuilding, and wrong as the way to change a running one.

### `configSubstitutions`

A secret such as a webhook URL should not sit in a ConfigMap. Put a placeholder in `config` and name the Secret key:

```yaml
config:
  webhooks:
    slackUrl: "@@SLACK_WEBHOOK@@"
configSubstitutions:
  - placeholder: "@@SLACK_WEBHOOK@@"
    secretKey: slack-webhook
```

An init container writes the Secret's value into the file the service reads, when the pod starts. Use plain tokens like `@@NAME@@`: the JSON the chart renders escapes `&`, `<` and `>`, so a placeholder containing one never matches. A value may only contain `A-Z a-z 0-9` and `: / . _ ~ % ? = & + # -`; any other character stops the pod from starting (so a bad value cannot corrupt the JSON). One trailing newline is dropped first, as `kubectl create secret --from-file` usually keeps the file's last one; a newline anywhere else is refused. The Secret is the one named by `secrets.existingSecret`, or the chart's own with the key in `secrets.values`.

## Secrets and GitOps

Unless you name your own, the chart generates the web token, the MCP token (in `<fullname>`) and the bundled store's password (in `<fullname>-store`). It looks the existing Secrets up in the cluster on each upgrade so the values stay, and marks them `helm.sh/resource-policy: keep` so an uninstall leaves them (and so the store's password still matches its volume; see [Uninstall](#uninstall)).

A tool that renders the chart without reading the cluster (`helm template`, which is what a GitOps controller runs) cannot see them, so it generates new tokens and a new store password on every sync. A new store password locks Darling out of a store that already has data. **For a GitOps install, create the Secrets yourself and set `secrets.existingSecret` and `store.bundled.auth.existingSecret` to their names.** Any secret manager can create them (External Secrets or Sealed Secrets, for example). `secrets.existingSecret` needs the token keys (`web-token`, `mcp-token`, or the names you set), the keys your `config` and `configSubstitutions` refer to, and for a `file:` reference the path `/run/secrets/darling/<key>`. The install notes print a warning whenever a generated value is in use.

A change to a Secret does not restart the pod; only a ConfigMap change does. After rotating a Secret, restart: `kubectl rollout restart deployment/<fullname>`.

**Rotating the bundled store's password.** The store sets its password once, when it creates its data directory; changing the Secret afterwards only locks Darling out. Change it inside the store first, then the Secret, then restart Darling:

```
kubectl exec <fullname>-store-0 -- psql -U darling -d darling -c "ALTER ROLE darling PASSWORD '<new password>'"
kubectl patch secret <fullname>-store -p '{"stringData":{"password":"<new password>"}}'
kubectl rollout restart deployment/<fullname>
```

(With `store.bundled.auth.existingSecret`, update that Secret instead.)

## The password key

Darling seals the passwords of the servers it monitors under a key. By default it makes one in its state volume. `passwordKey` keeps it in a Secret you manage instead, so a volume restored without the Secret cannot read the stored passwords:

```yaml
passwordKey:
  existingSecret: darling-password-key
  secretKey: password-key
```

The chart mounts it read-only at `/run/secrets/darling-password-key/<secretKey>` with mode 0440 and group 1654 (through `fsGroup`), outside `credentials/`, and sets `passwordKey` to that `file:` path. `source: env` injects it as `DARLING_PASSWORD_KEY` instead; `file:` is the safer one (an environment variable shows in `kubectl exec … env` and `/proc`).

**Making a key:** run the image once as user 1654 with the service's `--new-password-key` verb, and keep the key from its standard output (no volume to mount, so it works the same under Docker Desktop):

```
docker run --rm --user 1654 --entrypoint sh ghcr.io/erikdarlingdata/performancemonitor-darling:<release> -c \
  'dotnet PerformanceMonitor.Darling.Service.dll --new-password-key /tmp/password-key >&2 && cat /tmp/password-key' > password-key
kubectl create secret generic darling-password-key --from-file=password-key=password-key
rm password-key
```

The verb prints the key's fingerprint on standard error; the service logs the same fingerprint when it loads the key. `openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 -out password-key.pem` makes an equivalent key. The key must be RSA-3072. Put it in your secret store (or the Secret, as above) and delete the file. See [`--new-password-key`](../../../docs/darling/cli.md#the-table).

**Moving an existing key off the volume:** copy the `password-key` file from the credentials directory into the Secret, set `passwordKey`, upgrade, and check the log shows the same fingerprint loaded from `passwordKey` before you delete the old file. The steps, the checks the service makes on the file, and `--reset-password-key` are in [Keeping the key outside the service](../../../docs/darling/security.md#security--least-privilege-roles).

## Readiness

The chart sets a `health` block in `darling.json` and probes the health port, 5154:

| Probe | Path | Notes |
|---|---|---|
| startup and liveness | `/livez` | Answers 200 in every state, including `stopped`, so it only catches a hung process. A long schema migration never fails it. |
| readiness | `/readyz` | 200 only while the collection loop is running. |

There is no liveness probe on the dashboard or MCP port, and no probe reads the stopped marker.

**Not Ready** means the collector is `starting` (no verdict yet), `degraded` (a startup step failed and the service is retrying it) or `stopped` (a startup step failed for good, or the configuration has problems). The pod is never restarted for it: restarting does not fix a refused configuration or an unreachable store. `kubectl describe pod` shows the failed probe, and `kubectl logs` says why. Do not count on the dashboard meanwhile: on a store the chart's Darling does not manage, the dashboard waits for the store-login verdict before it starts, so while the collector is `starting` or `degraded` at start it may not answer, and the ownership stop of a root-owned volume (Upgrades, step 4) keeps it down on purpose. `kubectl logs` is the place to look. A Service sends no traffic to a pod that is not Ready, so an Ingress reaches nothing meanwhile; when the dashboard is up, a port-forward to the Deployment (or the pod), as in the install section, still reaches it.

What readiness does not cover: it follows a startup verdict. The service publishes that it is collecting once, when startup finishes, and does not change it back, so a store that becomes unreachable after collection started is not shown as not Ready. Watch collection itself (the dashboard's state, or the service log) for that. See [the health port](../../../docs/darling/network-endpoints.md#the-health-port) for what the port answers.

## Upgrades

1. **Back up the store first.** The bundled store has no backups of its own. Take a `pg_dump` (or a volume snapshot) before changing the chart or the image. An external store follows your own backup plan.
2. **`Recreate` means a short outage.** The state volume allows one writer, so the old pod stops before the new one starts. Collection pauses for that time.
3. Set the new `image.tag` and run `helm upgrade darling Darling/helm/performancemonitor-darling --reuse-values --set-string image.tag=<release>` (or edit your values file). If a new chart version adds values, drop `--reuse-values` and pass your values file.
4. **A volume from a deployment that ran the image as root.** The image runs as user 1654. A state volume left owned by root makes the new pod stop collection and report not Ready, never restarted, with the command to fix it in its log. Run a one-off Job (or initContainer) as root that does `chown -R 1654:1654 /var/lib/darling` and the `chmod`s of the volume root, with the Deployment scaled to 0 for a ReadWriteOnce volume, then scale back. The exact commands and the shapes of volume this covers (a new volume, a root-owned one, one mounted at `credentials/`) are in [Container user](../../../docs/darling/security.md#container-user), which also has the raw-manifest `securityContext` this chart renders. The chart itself never runs as root.
5. **The bundled store's PostgreSQL major version.** A chart version that moves `store.bundled.image` to a new PostgreSQL major is a breaking change: PostgreSQL cannot open a data directory from another major. Dump the store with the old image, then restore it into the new one.

## Uninstall

`helm uninstall` leaves four things behind on purpose: the state PVC `<fullname>`, the bundled store's volume `data-<fullname>-store-0`, the chart Secret `<fullname>` and the store Secret `<fullname>-store` (the chart created the last two only when you named no Secret of your own). They belong together: the state volume holds the password key that opens the passwords saved in the store, and the store Secret holds the password the store's volume was created with. A reinstall with the same release name picks all four up again. To start over, delete all of them; never delete some and keep the rest:

```
kubectl delete pvc <fullname> data-<fullname>-store-0
kubectl delete secret <fullname> <fullname>-store
```

With a long release name the store's names are shortened (`kubectl get pvc,secret` shows them). With `persistence.existingClaim` the state volume is yours and the chart never touches it.

## Tests

`tests/render-tests.sh` renders the chart and checks it without a cluster (it needs `helm`). `tests/kind-tests.sh` installs both store modes on a kind cluster. Both run in `.github/workflows/darling-helm.yml`.
