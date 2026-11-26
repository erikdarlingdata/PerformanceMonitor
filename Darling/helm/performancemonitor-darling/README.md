# performancemonitor-darling

A Helm chart for Darling, the collector service of PerformanceMonitor. It runs the service as one pod with a state volume, a Service for the web dashboard (and, when you turn it on, the MCP endpoint), optional Ingresses, and either a single bundled TimescaleDB pod or a connection to a PostgreSQL server you run.

Requires Kubernetes 1.27 or later, Helm 3, and a Darling image newer than 3.12.0: the probes use the health port, which first shipped in the release after 3.12.0, so 3.12.0 and older do not work (the chart refuses a plain release tag of 3.12.0 or older). Nightly tags (`3.12.0-nightly.<date>.<n>`) are not checked, and a nightly built before the health port restarts in a loop: pin a release. `latest` moves with each release; pin `image.tag` to a release you have tested when you install from a checkout.

Each release publishes the chart, starting with the first release that includes the publishing workflow (`.github/workflows/darling-helm-release.yml`): as an OCI chart at `oci://ghcr.io/erikdarlingdata/charts/performancemonitor-darling`, with a keyless cosign signature, and as a `.tgz` with its checksum on the GitHub release. The published chart's version is the release version, and it pins that release's image by tag and digest, so it needs no `image.tag`.

Every value is documented in [`values.yaml`](values.yaml) and checked by [`values.schema.json`](values.schema.json). This page covers what the values do together.

## Install

```
helm install darling oci://ghcr.io/erikdarlingdata/charts/performancemonitor-darling --version <release>
```

`<release>` is a release version without the `v`, for example `3.14.0`. To check the chart's signature before installing, see [Verify the chart](../../../docs/darling/kubernetes.md#verify-the-chart).

### Offline install

Download `performancemonitor-darling-<release>.tgz` and `SHA256SUMS-helm.txt` from the release's assets, check the package, and install from the file:

```
sha256sum -c SHA256SUMS-helm.txt
helm install darling ./performancemonitor-darling-<release>.tgz
```

## Install from a checkout

For a commit that is not released yet, or to work on the chart itself, install from a checkout of this repository:

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

**Bundled** runs one TimescaleDB pod (a StatefulSet) with the release. It suits evaluation and small installs: one replica, no backups, no failover. The image is `timescale/timescaledb:2.32.1-pg17`, the image CI tests Darling against, with the same server settings as the compose file's store. PostgreSQL's own log is in the `log` folder of the store volume, and `kubectl logs` for the store pod shows only its startup lines. Darling's connection string carries no password. The password reaches the Darling pod as `PGPASSWORD` from the `password` key of the store Secret, the same key the store pod reads. The chart generates that Secret (`<fullname>-store`) unless you name your own in `store.bundled.auth.existingSecret`.

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

### Custom alerts on an external store

The bundled store runs custom alerts. An external store runs them on the `web` role, so it needs the current `provision-roles.sql` and `postgres.webConnectionString`. With `store.provisionRoles.enabled: true` and `store.provisionRoles.wireConnectionStrings: true`, the chart does both (the next section says how). Without them, the service logs "Custom alert evaluator not started" and names the setting, and the built-in alerts still run ([Security](../../../docs/darling/security.md#security--least-privilege-roles) describes the roles).

### Store roles on an external store

On an external store the Darling login is the database owner, and the service never creates the four least-privilege roles (`admin`, `viewer`, `web`, `mcp`) or the store functions that [`tools/provision-roles.sql`](../../tools/provision-roles.sql) creates. Almost every release adds grants or functions that only that script creates, so someone has to run it again after each upgrade. With `store.provisionRoles.enabled: true` the chart runs it for you, in the same install or upgrade (and on every Argo CD sync). It is off by default and never runs against the bundled store, which provisions its own roles (the render fails there).

**What runs.** A Helm hook Job `<fullname>-store-roles` (`post-install,post-upgrade`; Argo CD maps it to `PostSync`):

1. The init container `prepare` is the release's own Darling image running `--prepare-provision-roles`. It reads the owner's login and the four role passwords from mounted files, waits until the store has reached this release's schema (so the Job never runs against a store the new pod has not migrated yet), checks that the login owns every object in the `collect` and `config` schemas, and writes the shipped script with the owner, the database and the passwords filled in (as SCRAM-SHA-256 verifiers, never plain text), a pgpass file and the libpq settings into a memory-backed `emptyDir`.
2. The container `provision-roles` runs `psql -X -v ON_ERROR_STOP=1 -c "SET lock_timeout = '120s'" -f /work/provision-roles.sql` from a PostgreSQL client image (`store.provisionRoles.psqlImage`, default `store.bundled.image`). Any error stops psql with exit code 3, so a failed run fails the release (or the sync), and the script's message is in the Job log. The Job log masks every verifier. The last line is `provision-roles.sql exit code: N`. psql's output is printed after psql exits, so a statement that waits on another session's lock would leave the log empty if the pod hit its deadline (`waitSeconds` + 300 s) first; the 120 s `lock_timeout` makes it fail with `canceling statement due to lock timeout` (exit 3) well before that. The script also drops every `PG*` variable the container inherited before it loads `libpq.env`, so the connection comes only from the files the init container wrote.

The Job runs without a service account token. The login Secret and the role-password Secret are mounted only into the init container, as files: no env variable or argument carries a password, and psql gets its password from the pgpass file. The Job keeps its last run and log until the next one.

```yaml
store:
  mode: external
  external:
    host: db.example.internal
    database: darling
    username: darling
    sslMode: VerifyFull
    passwordSecret: { name: darling-secrets, key: sql-password }
    caSecret: { name: darling-store-ca, key: ca.crt }   # psql needs a root certificate file for VerifyCA and VerifyFull
  provisionRoles:
    enabled: true
    login:
      passwordSecret: { name: darling-secrets, key: sql-password }   # the service's own Secret and key, so the owner password has one copy
    rolePasswordsSecret: darling-store-roles
```

`login` takes exactly one of `passwordSecret` (the owner's password; host, port, database, username, `sslMode` and `caSecret` come from `store.external`) or `connectionStringSecret` (a whole Npgsql connection string for the owner, with no client certificate and, if it names a `Root Certificate`, only `/etc/darling/store-ca/ca.crt`, where the Job mounts `store.external.caSecret`). Either may name the Secret and key the service's own connection uses, so the owner password has one copy. `passwordSecret` cannot be combined with `store.external.connectionStringSecret`, because there is no host to pair it with, and `store.external.host` must be one host (a comma-separated list is refused). With `sslMode` `VerifyCA` or `VerifyFull`, psql (libpq) needs a root certificate file, where the service itself falls back to the operating system's trust store: set `store.external.caSecret` (the render fails without it), or put a `Root Certificate` in the connection string of a `connectionStringSecret` login (the verb refuses it without one).

`rolePasswordsSecret` names a Secret you manage with the keys `admin`, `viewer`, `web` and `mcp`. The chart generates no passwords. Each one needs 16 or more characters from `A-Z a-z 0-9 . _ ~ + / = -`; the Job refuses anything else (exit code 2), naming the role and never the value.

**Wiring the web and MCP logins.** `store.provisionRoles.wireConnectionStrings` (default `true` while the Job is on) makes the dashboard and the MCP endpoint connect as the `web` and `mcp` roles instead of the owner. A second init container, `store-logins`, runs in the Darling image and reads only the `web` and `mcp` keys of `rolePasswordsSecret`, mounted as files. It applies the same password rule (exit code 2, naming the role, never the value) and writes the two whole connection strings, built from the host-based `store.external` settings (host, port, database, `sslMode` and, with `caSecret`, `Root Certificate=/etc/darling/store-ca/ca.crt`), to a memory-backed `emptyDir` at `/run/darling-store-logins`, mode 0400. The chart then sets `postgres.webConnectionString` and `postgres.mcpConnectionString` in `darling.json` to `file:/run/darling-store-logins/web` and `.../mcp`. The Darling container mounts that volume read-only. It never sees `rolePasswordsSecret`: not in its environment, and not as a file other than the two finished strings. While wiring is on, the chart owns those two keys, so setting either inside `config` fails the render. Wiring needs the host-based `store.external` settings: with `store.external.connectionStringSecret` the render fails too, because there is no host to put in the strings. Set `wireConnectionStrings: false` and point `config.postgres.webConnectionString` and `config.postgres.mcpConnectionString` at your own `file:` references.

On the first install the dashboard and the MCP endpoint start before the Job has created the roles, so their store reads fail until it has. Then they succeed with no restart, because each read opens a pooled connection.

**Rotating a role password.** Change the value in `rolePasswordsSecret` and upgrade (or sync) so the Job sets the new password on the role. Then restart the pod (`kubectl rollout restart deployment/<fullname>`) so the `store-logins` init container rewrites the two files. Between the Job and the restart, the running pod still holds the old `web` and `mcp` passwords, and new connections of that surface fail. Pooled connections already open keep working until they close. The restart ends the window, so do it right after the upgrade.

**The owner's rights.** The Job logs in as the store owner, not as a superuser, and the service's own connection keeps the rights it has today. The owner must hold `CREATEROLE`, and it keeps it for as long as the Job is on:

- On PostgreSQL 15 and earlier, `CREATEROLE` can grant membership in any role that is not a superuser, `pg_execute_server_program` and `pg_write_server_files` included. There it is close to superuser, so weigh it before turning the Job on.
- From PostgreSQL 16, `CREATEROLE` covers only the roles the owner created or holds `ADMIN OPTION` on. The owner holds that option on a role it created itself. For a role a superuser made, run once as a superuser:

  ```sql
  GRANT admin, viewer, web, mcp TO "<owner>" WITH ADMIN OPTION;
  ```

  The script's guard checks this before it sends a password, and stops the run (exit code 3) with the roles and the statements to run.

An owner without `CREATEROLE` stops the run at once, before any role is created, with the guard's message (exit code 3).

With CloudNativePG, declare the owner with `CREATEROLE` in the Cluster:

```yaml
spec:
  managed:
    roles:
      - name: darling            # the role that owns the database and that the service logs in as
        ensure: present
        login: true
        createrole: true
        passwordSecret:
          name: darling-owner
```

**One WARNING is expected.** Nine `ALTER ROLE ... SET` lines of the script (`temp_file_limit`, `log_min_duration_statement` and `log_parameter_max_length` for `viewer`, `web` and `mcp`) need a superuser. An owner run skips them and prints exactly one WARNING containing `superuser-only settings`, which names each role still missing a setting and gives the `ALTER ROLE ... SET` statements to run. Run those once as a superuser and the WARNING is gone from every later run. The owner may also print non-fatal WARNINGs that it is a member of `darling_alert_sources`; they need no action, unless your `pg_hba.conf` has a `+darling_alert_sources` line (the WARNING says what to add).

**When it fails.** `kubectl logs job/<fullname>-store-roles -c prepare` is the verb, `-c provision-roles` is psql. Exit codes:

| Code | Where | Meaning |
|---|---|---|
| 1 | `prepare` | Unexpected: the store refused the login, the shipped script no longer matches what the verb expects, a file could not be written. |
| 2 | `prepare` | A usage or input error: a missing, short or bad-character role password, or a connection string the Job cannot use. The message names the role or the setting. |
| 3 | `provision-roles` | psql stopped: the script's own guard (with its message and the statements to run), or any other error, because `ON_ERROR_STOP` is on. |
| 4 | `prepare` | The store did not reach this release's schema within `waitSeconds`. The usual cause is that the new pod has not finished migrating. The message names the version it last saw and the one it waited for, or, when it never read a version, the last connection error. |
| 5 | `prepare` | The login does not own every object in the `collect` and `config` schemas (a superuser ran the script by hand once). The message lists each object with its owner and the `ALTER ... OWNER TO` statements a superuser runs once. |
| 1 or 2 | `provision-roles` | psql's own exit code: it could not read a file or connect. libpq's message in the log says why (a missing root certificate file, a refused login, an unreachable host). |

The verb sends no DDL, so a failure in `prepare` leaves the store untouched.

**Around the Job.**

- The Job pod's labels differ from the Darling pod's on purpose (it must never match the web or MCP Service), and `podLabels` and `podAnnotations` are not passed to it. A NetworkPolicy written for the Darling pod does not cover it: the Job needs its own egress to the store. An Istio sidecar injected into the Job would keep it from completing, so exclude it from injection (for example with the injector's `neverInjectSelector` on `app.kubernetes.io/component=store-roles`).
- With wiring on, a missing `web` or `mcp` key, or a password that breaks the role-password rule, stops the Darling pod itself from starting (its `store-logins` init container refuses it). Under the `Recreate` strategy the old pod is already gone by then.
- `helm uninstall` leaves the hook Job in place, as Helm does for hooks; delete it with `kubectl delete job <fullname>-store-roles`.
- The script revokes `PUBLIC`'s `CONNECT` on the database on every run, so any other login that connects to the store needs its own `GRANT CONNECT ON DATABASE`.

**Timing.** Helm waits for a hook Job inside `--timeout` (5 minutes by default), and the Job's `activeDeadlineSeconds` is `waitSeconds + 300`. A release that migrates for a long time needs `helm upgrade --timeout` to cover `waitSeconds` (default 900). One migration step can run for up to 30 minutes on a large store, and the pod's log names that step when it starts. For an upgrade that large, set `store.provisionRoles.waitSeconds` above 1800. Without `--wait`, the Job starts as soon as the manifests are applied and waits for the new pod to migrate the store.

**Tested versions.** PostgreSQL 18.7 (the live tests); PostgreSQL 17 and 15 (the kind cases `external_provision_roles` and `provision_roles_fails_the_release`).

## Values

| Value | Default | What it does |
|---|---|---|
| `image.repository`, `image.tag`, `image.digest`, `image.pullPolicy` | `ghcr.io/erikdarlingdata/performancemonitor-darling`, the chart's `appVersion` (`latest`), none, `IfNotPresent` | The image. Pin the tag. |
| `imagePullSecrets` | `[]` | Pull Secrets for a private registry. |
| `nameOverride`, `fullnameOverride` | empty | Resource naming. |
| `resources` | requests 250m CPU and 512Mi, limit 4Gi memory | The Darling container. |
| `persistence.size`, `.storageClass`, `.existingClaim` | `1Gi`, cluster default, none | The state volume at `/var/lib/darling`. An existing claim must be writable by user 1654. |
| `deploymentAnnotations` | empty | Annotations on the Deployment itself, for controllers that watch them. |
| `podAnnotations`, `podLabels`, `nodeSelector`, `tolerations`, `affinity`, `priorityClassName`, `hostAliases` | empty | Pod placement and metadata. |
| `extraEnv`, `extraEnvFrom`, `extraVolumes`, `extraVolumeMounts` | empty | Extras for the Darling container. |
| `config` | `{}` | Your `darling.json`: servers, collection, alerts. The chart owns some keys (below). |
| `configSubstitutions` | `[]` | `{ placeholder, secretKey }` pairs: secrets written into `darling.json` when the pod starts. |
| `passwordKey.existingSecret`, `.secretKey`, `.source` | none, `password-key`, `file` | Keep the key that seals stored server passwords in a Secret. |
| `secrets.existingSecret`, `.values`, `.webTokenKey`, `.mcpTokenKey`, `.metricsTokenKey` | none, `{}`, `web-token`, `mcp-token`, `metrics-token` | The chart Secret: tokens and your own keys. |
| `store.mode` | `bundled` | `bundled` or `external`. |
| `store.bundled.image`, `.persistence`, `.auth.existingSecret`, `.resources` | the image above, `10Gi`, none, requests 2Gi and limit 4Gi memory | The bundled store. |
| `store.external.*` | empty | The external store (above). |
| `store.provisionRoles.*` | disabled | The hook Job that runs `provision-roles.sql` on an external store ([Store roles on an external store](#store-roles-on-an-external-store)). |
| `network.allowFrom` | `[]` | Client CIDRs the dashboard and MCP endpoint admit, unless a surface sets its own. Empty renders `127.0.0.1/32`. |
| `network.trustedProxies` | `[]` | Proxy CIDRs the dashboard trusts `X-Forwarded-*` from. |
| `web.enabled`, `web.port`, `web.publicBaseUrl` | `true`, `5153`, derived | The dashboard. |
| `web.network.*`, `web.ingress.*` | empty, disabled | Per-surface overrides, and the dashboard Ingress. |
| `mcp.enabled`, `mcp.port` | `false`, `5152` | The MCP endpoint. |
| `mcp.network.allowFrom`, `mcp.ingress.*` | empty, disabled | Override, and the MCP Ingress. |
| `metrics.enabled`, `metrics.port` | `false`, `5155` | The Prometheus `/metrics` port (needs an image newer than 3.13.0). Its token comes from the chart Secret. |
| `metrics.allowFrom` | empty | Addresses allowed to scrape; empty adds none. `network.allowFrom` never applies to metrics. |
| `metrics.serviceMonitor.enabled`, `.interval`, `.labels` | `false`, `60s`, `{}` | A Prometheus Operator ServiceMonitor for the metrics Service (needs `metrics.enabled` and the CRD). |

Fixed, not values: one replica, the `Recreate` strategy, and the pod security context (`runAsNonRoot`, user, group and `fsGroup` 1654, `fsGroupChangePolicy: OnRootMismatch`, `RuntimeDefault` seccomp). Every container drops all capabilities, forbids privilege escalation and has a read-only root filesystem. No container ever runs as root. The init container that writes `darling.json` has small fixed resources (requests 10m CPU and 16Mi, limit 64Mi), so a namespace with a ResourceQuota accepts the pod.

### Keys the chart owns in `config`

The chart owns exactly these keys of `darling.json`:

- `postgres.managed` and `postgres.connectionString` (from `store.*`);
- `web.enabled`, `web.port`, `web.publicBaseUrl`, `web.network.listen`, `web.network.allowFrom`, `web.network.trustedProxies` and `web.network.token`;
- `mcp.enabled`, `mcp.port`, `mcp.network.listen`, `mcp.network.allowFrom` and `mcp.network.token`;
- `metrics.enabled`, `metrics.port`, `metrics.listen`, `metrics.allowFrom` and `metrics.token`;
- `health` and `passwordKey`.

Setting one inside `config` fails the render and names the value to use instead (or says the chart sets it and nothing overrides it); the chart never overrides one silently. Keys are matched ignoring case, as Darling reads them, and `config` must spell the top-level `postgres`, `web`, `mcp` and `metrics` exactly so: a `Postgres` block would sit beside the chart's and Darling would read only one of the two, so the render fails. The same holds for `network` inside `web` and `mcp`: a `config.web.Network` block would sit beside the chart's `network` block, and its `tls` or `oidc` settings would be dropped without a message, so the render fails. Every other key under `postgres`, `web.network` and `mcp.network` is yours and is passed through, for example `postgres.webConnectionString`, `web.network.oidc`, `web.network.tls` and `mcp.network.tls`. The chart sets `mcp.network.hostName` only when `config` does not (see below).

### Through `config`

Some settings have no chart value; they go in `config`, next to the keys the chart owns.

- **OIDC sign-in for the dashboard:** `config.web.network.oidc` (`authority`, `clientId`, `clientSecret`). Give the client secret as a `file:` reference into the chart Secret, for example `clientSecret: file:/run/secrets/darling/oidc-client-secret` with the key `oidc-client-secret` in `secrets.values` or in your `existingSecret`. Behind an Ingress the dashboard needs `trustedProxies` for the `https` redirect.
- **TLS on the pod itself** (instead of, or behind, a TLS-terminating Ingress): mount the certificate with `extraVolumes` and `extraVolumeMounts`, and set `config.web.network.tls` or `config.mcp.network.tls` (`certPath` and `keyPath`, or `pfxPath` and `pfxPassword`) to the mounted paths. The Ingress then has to speak HTTPS to the pod (an annotation of your controller).
- **Least-privilege store logins:** with `store.provisionRoles.enabled` and `wireConnectionStrings` (the default), the chart creates the roles and owns `postgres.webConnectionString` and `postgres.mcpConnectionString` itself ([Store roles on an external store](#store-roles-on-an-external-store)). Otherwise the two keys are not owned: on an external store, run `tools/provision-roles.sql` (see [Security](../../../docs/darling/security.md#security--least-privilege-roles)), put the two connection strings in the chart Secret, and point the keys at them with `file:/run/secrets/darling/<key>`.

Two warnings in the log are expected with this chart:

- On an external store without those two strings, the dashboard and the MCP endpoint each log that they connect to the store "as the owner login (postgres.connectionString) because postgres.webConnectionString is not set" (or `mcpConnectionString`). That is the documented fallback, not a fault: they work, with the owner's rights. The strings above, or `store.provisionRoles` with its default wiring, remove it.
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
- `trustedProxies` set to the controller pods' CIDR trusts every pod in that CIDR: any of them can claim any client address in `X-Forwarded-For`. The health port, 5154, answers anyone who can route to the pod. A NetworkPolicy that admits only the ingress controller to the web and MCP ports, Prometheus to the metrics port, and nothing else to the pod, closes both (the kubelet's probes come from the node, which most network plugins always admit).
- IPv4 is assumed: the chart sets `listen` to `0.0.0.0`. IPv6-only clusters are not supported in this version.

### Prometheus metrics

`metrics.enabled=true` opens Darling's `/metrics` port (default 5155) and adds a ClusterIP Service for it, named `<fullname>-metrics`. The port serves the numbers of the dashboard's fleet page and per-server SQL Server gauges (CPU, memory, disk, waits, tempdb), labeled with the server's display name, in the Prometheus text format; [the metrics port](../../../docs/darling/network-endpoints.md#the-metrics-port) lists what it returns. It needs a Darling image newer than 3.13.0, and the chart refuses an older release tag.

- The port is protected by a bearer token, `secrets.metricsTokenKey` (default `metrics-token`) in the chart Secret, generated like the web and MCP tokens. With `secrets.existingSecret`, that Secret must hold the key. Read it with `kubectl get secret <fullname> -o jsonpath='{.data.metrics-token}' | base64 -d`.
- `network.allowFrom` never applies to metrics. `metrics.allowFrom` is the address filter of this port, a list of CIDRs (one address is `/32`; an entry without a prefix length fails the render, since Darling would turn the port off for it). Empty adds none, so the token is the only gate, and Darling logs a Warning at every start that the port is guarded by the token alone. Setting `metrics.allowFrom` to the Prometheus pods' CIDR removes that part. The port serves plain HTTP unless `web.network.tls` is set in `darling.json`, and while it does, Darling also warns at every start that the token crosses the network unencrypted. That warning is expected with the chart's default.
- A scrape reads an in-memory snapshot, never the store. A scrape starts a refresh only when the snapshot is at least 55 seconds old, so the store sees at most one refresh a minute and none while nobody scrapes. The default `metrics.serviceMonitor.interval` is therefore `60s`; scraping faster returns the same values.
- With the Prometheus Operator, `metrics.serviceMonitor.enabled=true` renders a ServiceMonitor that scrapes the Service with the chart's token (a `monitoring.coreos.com/v1` object, rendered without asking the cluster, so the CRD must exist where you apply it). Put the label your Prometheus selects on in `metrics.serviceMonitor.labels` (for kube-prometheus-stack, `release: <its release name>`); a key the chart also sets takes your value there, while the ServiceMonitor's selector keeps the chart's labels. `metrics.serviceMonitor.interval` takes any Prometheus duration (`30s`, `1m30s`). When `config.web.network.tls` is set, the metrics port serves HTTPS only and the ServiceMonitor scrapes with `scheme: https`. Put the scrape's CA or `serverName` in `metrics.serviceMonitor.tlsConfig` (a Prometheus Operator `tlsConfig`, rendered as given), unless the certificate already chains to a CA Prometheus trusts and matches the Service's host name. Enabling it without `metrics.enabled` fails the render.
- Without the Operator, point a plain scrape job at the Service, with the token in a file Prometheus can read. When `config.web.network.tls` is set, add `scheme: https` and a `tls_config` for the certificate (the sample shows plain HTTP):

```yaml
scrape_configs:
  - job_name: darling
    metrics_path: /metrics
    scrape_interval: 60s
    # scheme: https  # when config.web.network.tls is set; add tls_config: { ca_file: ..., server_name: ... } as needed
    authorization:
      credentials_file: /etc/prometheus/secrets/darling-metrics-token
    static_configs:
      - targets: ["<fullname>-metrics.<namespace>.svc:5155"]
```

## Which values the store overrides after the first start

`darling.json` is only a seed for some keys. On first start the service copies them into its store; from then on the store holds the only copy, and changing the value in the chart does nothing. The rest are read from the file at every start, and a chart change reaches them through the ConfigMap, which restarts the pod.

| Keys | Where they live | How to change them after the first start |
|---|---|---|
| `alerts`, `analysis` (`enabled`, `intervalMinutes`, `notificationsEnabled`, `notifySeverity`) | First-run seed | The Viewer's Settings, or the MCP tool `update_alert_settings` |
| `smtp`, `webhooks` (including a webhook set through `configSubstitutions`) | First-run seed | The Viewer's Settings > Notifications, or the MCP tool `update_notification_channels`. Rotating the Secret behind a webhook placeholder does not change a webhook already stored |
| `capturePlans`, `queryStoreBackfillEnabled`, `queryStoreTextBudgetMb`, `queryStoreFetchTopN`, `maxConcurrentSweeps` | First-run seed | The Viewer's Settings, or the MCP tool `update_service_flags` |
| `planContentRetentionDays`, `composeStatementTimeoutSeconds`, `planXmlCompression` | First-run seed | The Viewer's Settings window, or the MCP tool `update_service_flags`, or the column in `config.config_service` in the store |
| `servers` | First-run seed, copied once while the server registry is empty. A server added to the file later is ignored | The Viewer, the dashboard's Admin page, or the MCP tools `add_servers` and `edit_server` |
| `mcp.enabled`, `mcp.port`, `web.enabled`, `web.port` (the chart's `mcp.enabled`, `mcp.port`, `web.enabled`, `web.port`) | First-run seed | The Viewer's Settings, or `--enable-mcp` / `--disable-mcp` / `--enable-web` / `--disable-web` run in the pod (`kubectl exec`). Live, no restart. A port changed this way no longer matches the chart's Service and probes, so change the chart value too |
| `postgres` (the chart's `store.*`), `passwordKey`, `health`, `metrics` (the chart's `metrics.*`), `mcp.network` and `web.network` (the chart's `network.*`, `web.*`, `mcp.*`), and every other key of `config` not listed above | File only | Change the chart value and upgrade |

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

An init container writes the Secret's value into the file the service reads, when the pod starts. A placeholder must be `@@NAME@@`, with `NAME` made of `A-Z a-z 0-9 _` (the schema refuses any other shape), so it can never match part of a value the chart writes itself, such as `file:/run/secrets/darling/web-token`. A value may only contain `A-Z a-z 0-9` and `: / . _ ~ % ? = & + # -`; any other character stops the pod from starting (so a bad value cannot corrupt the JSON). One trailing newline is dropped first, as `kubectl create secret --from-file` usually keeps the file's last one; a newline anywhere else is refused. The Secret is the one named by `secrets.existingSecret`, or the chart's own with the key in `secrets.values`.

## Secrets and GitOps

Unless you name your own, the chart generates the web, MCP and metrics tokens (in `<fullname>`) and the bundled store's password (in `<fullname>-store`). It looks the existing Secrets up in the cluster on each upgrade so the values stay, and marks them `helm.sh/resource-policy: keep` so an uninstall leaves them (and so the store's password still matches its volume; see [Uninstall](#uninstall)).

A tool that renders the chart without reading the cluster (`helm template`, which is what a GitOps controller runs) cannot see them, so it generates new tokens and a new store password on every sync. A new store password locks Darling out of a store that already has data. **For a GitOps install, create the Secrets yourself and set `secrets.existingSecret` and `store.bundled.auth.existingSecret` to their names.** Any secret manager can create them (External Secrets or Sealed Secrets, for example). `secrets.existingSecret` needs the token keys (`web-token`, `mcp-token`, `metrics-token` with `metrics.enabled`, or the names you set), the keys your `config` and `configSubstitutions` refer to, and for a `file:` reference the path `/run/secrets/darling/<key>`. The install notes print a warning whenever a generated value is in use.

The chart mounts at `/run/secrets/darling` only the keys that the rendered `darling.json` references as `file:/run/secrets/darling/<key>`: the token keys of the enabled surfaces, and the keys your `config` names. A key used only by `configSubstitutions` reaches the init container as an environment variable and is never a file in the Darling container, so no server can name it as its password. A key counts only when it is a whole string value that starts with `file:`, the way Darling recognises a reference. Reference chart Secret keys only from settings Darling reads: a `file:` value under a key Darling does not know is mounted without being one of its own secrets. The dashboard and the MCP tools accept no `env:` or `file:` password. A server added later from the host command line with a `file:` password needs its own file, mounted with `extraVolumes` and `extraVolumeMounts`: every key `darling.json` references belongs to the service or to a server the file declares, and is refused for any other server. Every mounted key must exist in the Secret, or the pod does not start (the kubelet names the missing key in the pod's events).

A change to a Secret does not restart the pod; only a ConfigMap change does. After rotating a Secret, restart: `kubectl rollout restart deployment/<fullname>`. A controller that watches Deployment annotations can do it for you: set its annotation through `deploymentAnnotations` (Reloader is one example).

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

## Moving from your own manifests

Two things catch a deployment that ran Darling from manifests of its own before it moves to this chart.

**A Deployment with the chart's name must be deleted once.** When the old Deployment has the name the chart renders (for example with `fullnameOverride` set to the old name), the first install or sync fails: Helm will not take over a Deployment it did not create, and a sync cannot change the old Deployment's selector to the chart's (`app.kubernetes.io/name` and `app.kubernetes.io/instance`). Delete the old Deployment once (`kubectl delete deployment <name>`) and the chart's takes its place. That touches neither the state volume nor the Secrets, and with the `Recreate` strategy the gap is the same as any restart. A Service or ConfigMap of your own is only in the way when its name collides with one the chart renders; otherwise delete the old ones after the switch.

**Servers seeded before the chart keep their `file:` password paths.** `servers` in `darling.json` is a first-start seed (see [Which values the store overrides after the first start](#which-values-the-store-overrides-after-the-first-start)), and the store keeps each server's password reference as it was seeded. The chart mounts the Secret at `/run/secrets/darling/`, so a server seeded with a password under another path finds no file there. Mount the old path as well, with `extraVolumes` and `extraVolumeMounts`, and keep it for as long as those servers should read their passwords from the Secret. The Admin page and the MCP tool `edit_server` take only the password itself, not a `file:` reference, so a server edited there stops reading the Secret:

```yaml
extraVolumes:
  - name: old-secret-paths
    secret:
      secretName: <your-secret>
      defaultMode: 0440
      items:
        - key: sql-password-prod
          path: sql-password-prod
extraVolumeMounts:
  - name: old-secret-paths
    mountPath: /run/secrets/<old-path>
    readOnly: true
```

## Upgrades

1. **Back up the store first.** The bundled store has no backups of its own. Take a `pg_dump` (or a volume snapshot) before changing the chart or the image. An external store follows your own backup plan.
2. **`Recreate` means a short outage.** The state volume allows one writer, so the old pod stops before the new one starts. Collection pauses for that time.
3. With the published chart, run `helm upgrade darling oci://ghcr.io/erikdarlingdata/charts/performancemonitor-darling --version <new release> -f <your values>`: the new chart brings its own image pin, so leave both `image.tag` and `image.digest` out of your values. A digest left over from an earlier pin wins over the new chart's: it would render the new tag with the old digest and keep running the old image. To pin another image, set both. From a checkout, set the new `image.tag` and run `helm upgrade darling Darling/helm/performancemonitor-darling --reuse-values --set-string image.tag=<release>` (or edit your values file). If a new chart version adds values, drop `--reuse-values` and pass your values file.
4. **A volume from a deployment that ran the image as root.** The image runs as user 1654. A state volume left owned by root makes the new pod stop collection and report not Ready, never restarted, with the command to fix it in its log. Run a one-off Job (or initContainer) as root that does `chown -R 1654:1654 /var/lib/darling` and the `chmod`s of the volume root, with the Deployment scaled to 0 for a ReadWriteOnce volume, then scale back. The exact commands and the shapes of volume this covers (a new volume, a root-owned one, one mounted at `credentials/`) are in [Container user](../../../docs/darling/security.md#container-user), which also has the raw-manifest `securityContext` this chart renders. The chart itself never runs as root.
5. **Resizing the bundled store's volume.** Changing `store.bundled.persistence.size` makes `helm upgrade` fail: a StatefulSet's `volumeClaimTemplates` cannot change. Resize the PVC itself instead: when its StorageClass has `allowVolumeExpansion: true`, raise its request with `kubectl patch pvc data-<fullname>-store-0 -p '{"spec":{"resources":{"requests":{"storage":"20Gi"}}}}'`, and keep the old size in your values.
6. **The bundled store's PostgreSQL major version.** A chart version that moves `store.bundled.image` to a new PostgreSQL major is a breaking change: PostgreSQL cannot open a data directory from another major. Dump the store with the old image, then restore it into the new one.

## Uninstall

`helm uninstall` leaves four things behind on purpose: the state PVC `<fullname>`, the bundled store's volume `data-<fullname>-store-0`, the chart Secret `<fullname>` and the store Secret `<fullname>-store` (the chart created the last two only when you named no Secret of your own). They belong together: the state volume holds the password key that opens the passwords saved in the store, and the store Secret holds the password the store's volume was created with. A reinstall with the same release name picks all four up again. To start over, delete all of them; never delete some and keep the rest:

```
kubectl delete pvc <fullname> data-<fullname>-store-0
kubectl delete secret <fullname> <fullname>-store
```

With a long release name the store's names are shortened (`kubectl get pvc,secret` shows them). With `persistence.existingClaim` the state volume is yours and the chart never touches it.

## Tests

`tests/render-tests.sh` renders the chart and checks it without a cluster (it needs `helm`). `tests/kind-tests.sh` installs both store modes on a kind cluster. `tests/release-tests.sh` checks how a release stamps and packages the chart (`.github/scripts/darling-helm-release.sh`), without a cluster or a registry. All of them run in `.github/workflows/darling-helm.yml`.
