#!/usr/bin/env bash
# Render tests of the chart. Run from anywhere: bash Darling/helm/performancemonitor-darling/tests/render-tests.sh
set -euo pipefail
. "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

DEPLOY="${RELEASE}-performancemonitor-darling"

# Evaluates a yq expression on the Darling Deployment.
dq() {
  doc Deployment "${DEPLOY}" | yq -o=json -I=0 "$1"
}

t pod_security_context_matches_8141
render
assert_eq "$(dq '.spec.template.spec.securityContext')" \
  '{"runAsNonRoot":true,"runAsUser":1654,"runAsGroup":1654,"fsGroup":1654,"fsGroupChangePolicy":"OnRootMismatch","seccompProfile":{"type":"RuntimeDefault"}}' \
  "pod securityContext"
assert_eq "$(dq '.spec.template.spec.automountServiceAccountToken')" "false" "automountServiceAccountToken"
assert_eq "$(dq '.spec.template.spec.enableServiceLinks')" "false" "enableServiceLinks"

t replicas_and_strategy_fixed
render
assert_eq "$(dq '.spec.replicas')" "1" "replicas"
assert_eq "$(dq '.spec.strategy.type')" '"Recreate"' "strategy"
render --set replicas=3
assert_eq "$(dq '.spec.replicas')" "1" "replicas with --set replicas=3"
assert_eq "$(dq '.spec.strategy.type')" '"Recreate"' "strategy with --set replicas=3"

t no_root_anywhere
render
assert_eq "$(dq '[.spec.template.spec.containers[], .spec.template.spec.initContainers[]] | map(select(.securityContext.allowPrivilegeEscalation != false)) | length')" "0" "allowPrivilegeEscalation"
assert_eq "$(dq '[.spec.template.spec.containers[], .spec.template.spec.initContainers[]] | map(select(.securityContext.readOnlyRootFilesystem != true)) | length')" "0" "readOnlyRootFilesystem"
assert_eq "$(dq '[.spec.template.spec.containers[], .spec.template.spec.initContainers[]] | map(select((.securityContext.capabilities.drop | join(",")) != "ALL")) | length')" "0" "capabilities.drop"
assert_eq "$(dq '[.spec.template.spec.containers[], .spec.template.spec.initContainers[]] | map(select(.securityContext.runAsUser == 0)) | length')" "0" "runAsUser 0"
assert_eq "$(dq '.spec.template.spec.securityContext.runAsUser')" "1654" "pod runAsUser"

t mounts_and_env
render
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts | map(.mountPath) | sort | join(",")' | tr -d '"')" \
  "/config,/run/secrets/darling,/tmp,/usr/share/PerformanceMonitorDarling,/var/lib/darling" "mount paths"
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts | map(select(.mountPath == "/var/lib/darling/credentials")) | length')" "0" "no credentials mount"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "state") | .persistentVolumeClaim.claimName')" "\"${DEPLOY}\"" "state claim"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "config") | .emptyDir.sizeLimit')" '"1Mi"' "/config size"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "app-share") | .emptyDir.sizeLimit')" '"512Mi"' "/usr/share size"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "tmp") | .emptyDir.sizeLimit')" '"256Mi"' "/tmp size"
# Each mount names the volume the table says.
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts[] | select(.mountPath == "/var/lib/darling") | .name')" '"state"' "state mount volume"
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts[] | select(.mountPath == "/config") | .name')" '"config"' "config mount volume"
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts[] | select(.mountPath == "/usr/share/PerformanceMonitorDarling") | .name')" '"app-share"' "app-share mount volume"
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts[] | select(.mountPath == "/tmp") | .name')" '"tmp"' "tmp mount volume"
assert_eq "$(dq '.spec.template.spec.containers[0].env[] | select(.name == "DARLING_CONFIG") | .value')" '"/config/darling.json"' "DARLING_CONFIG"
assert_eq "$(dq '.spec.template.spec.containers[0].env[] | select(.name == "DARLING_STOPPED_MARKER") | .value')" '"/tmp/darling-stopped"' "DARLING_STOPPED_MARKER"
assert_eq "$(doc PersistentVolumeClaim "${DEPLOY}" | yq '.spec.resources.requests.storage')" "1Gi" "pvc size"

t image_tag_and_digest
render
assert_eq "$(dq '.spec.template.spec.containers[0].image')" '"ghcr.io/erikdarlingdata/performancemonitor-darling:latest"' "default image"
render --set image.tag=3.13.0,image.digest=sha256:abc
assert_eq "$(dq '.spec.template.spec.containers[0].image')" '"ghcr.io/erikdarlingdata/performancemonitor-darling:3.13.0@sha256:abc"' "tag and digest"

t image_tag_numeric
# --set parses an all-digit tag as a number; it must still render as the tag, never as %!s(int64=...).
render --set image.tag=8017
assert_eq "$(dq '.spec.template.spec.containers[0].image')" '"ghcr.io/erikdarlingdata/performancemonitor-darling:8017"' "numeric tag"
assert_eq "$(dq '.spec.template.spec.initContainers[0].image')" '"ghcr.io/erikdarlingdata/performancemonitor-darling:8017"' "numeric tag, init container"

t resources_default
render
assert_eq "$(dq '.spec.template.spec.containers[0].resources')" \
  '{"limits":{"memory":"4Gi"},"requests":{"cpu":"250m","memory":"512Mi"}}' "darling resources"

t passthroughs
render \
  --set-json 'hostAliases=[{"ip":"10.0.0.9","hostnames":["db.internal"]}]' \
  --set-json 'podAnnotations={"a/b":"c"}' \
  --set-json 'podLabels={"team":"dba"}' \
  --set-json 'nodeSelector={"disk":"ssd"}' \
  --set-json 'tolerations=[{"key":"k","operator":"Exists"}]' \
  --set-json 'affinity={"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"z","operator":"Exists"}]}]}}}' \
  --set priorityClassName=high \
  --set-json 'imagePullSecrets=[{"name":"regcred"}]' \
  --set-json 'extraEnv=[{"name":"X","value":"1"}]' \
  --set-json 'extraEnvFrom=[{"secretRef":{"name":"more"}}]' \
  --set-json 'extraVolumes=[{"name":"extra","emptyDir":{}}]' \
  --set-json 'extraVolumeMounts=[{"name":"extra","mountPath":"/extra"}]'
assert_eq "$(dq '.spec.template.spec.hostAliases[0].hostnames[0]')" '"db.internal"' "hostAliases"
assert_eq "$(dq '.spec.template.metadata.annotations["a/b"]')" '"c"' "podAnnotations"
assert_eq "$(dq '.spec.template.metadata.labels.team')" '"dba"' "podLabels"
assert_eq "$(dq '.spec.template.metadata.labels["app.kubernetes.io/name"]')" '"performancemonitor-darling"' "selector labels kept beside podLabels"
assert_eq "$(dq '.spec.template.spec.nodeSelector.disk')" '"ssd"' "nodeSelector"
assert_eq "$(dq '.spec.template.spec.tolerations[0].key')" '"k"' "tolerations"
assert_eq "$(dq '.spec.template.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchExpressions[0].key')" '"z"' "affinity"
assert_eq "$(dq '.spec.template.spec.priorityClassName')" '"high"' "priorityClassName"
assert_eq "$(dq '.spec.template.spec.imagePullSecrets[0].name')" '"regcred"' "imagePullSecrets"
assert_eq "$(dq '.spec.template.spec.containers[0].env[] | select(.name == "X") | .value')" '"1"' "extraEnv"
assert_eq "$(dq '.spec.template.spec.containers[0].envFrom[0].secretRef.name')" '"more"' "extraEnvFrom"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "extra") | .name')" '"extra"' "extraVolumes"
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts[] | select(.name == "extra") | .mountPath')" '"/extra"' "extraVolumeMounts"

t existing_claim
render --set persistence.existingClaim=x
assert_eq "$(yq 'select(.kind == "PersistentVolumeClaim") | .metadata.name' "${OUT}")" "" "no PVC rendered"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "state") | .persistentVolumeClaim.claimName')" '"x"' "claim x"

# The rendered darling.json of the ConfigMap, as JSON.
config_json() {
  doc ConfigMap "${DEPLOY}" | yq '.data["darling.json"]' | yq -p json -o=json -I=0 "$1"
}

t config_merges_owned_keys
render --set 'config.servers[0].name=prod' --set 'config.servers[0].host=db.example'
assert_eq "$(config_json '.postgres.managed')" "false" "postgres.managed"
assert_eq "$(config_json '.servers')" '[{"host":"db.example","name":"prod"}]' "operator servers untouched"

t fail_owned_key_in_config
assert_fails "config.postgres.connectionString" --set config.postgres.connectionString=x
assert_fails "store." --set config.postgres.connectionString=x
assert_fails "config.postgres.managed" --set config.postgres.managed=true
assert_fails "config.web.network.token" --set config.web.network.token=x
assert_fails "network." --set config.web.network.listen=0.0.0.0
assert_fails "config.mcp.port" --set config.mcp.port=1
assert_fails "config.web.publicBaseUrl" --set config.web.publicBaseUrl=https://x
assert_fails "config.health" --set config.health.port=1
assert_fails "passwordKey." --set config.passwordKey=x

t secret_existing_vs_created
render --set secrets.existingSecret=s
assert_eq "$(yq "select(.kind == \"Secret\" and .metadata.name == \"${DEPLOY}\") | .metadata.name" "${OUT}")" "" "no chart Secret with existingSecret"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "secrets") | .secret.secretName')" '"s"' "secret volume uses s"
render
assert_eq "$(doc Secret "${DEPLOY}" | yq ".data | has(\"web-token\")")" "true" "generated web-token"
assert_eq "$(doc Secret "${DEPLOY}" | yq ".data | has(\"mcp-token\")")" "true" "generated mcp-token"
assert_eq "$(doc Secret "${DEPLOY}" | yq '.metadata.annotations["helm.sh/resource-policy"]')" "keep" "keep annotation"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "secrets") | .secret.secretName')" "\"${DEPLOY}\"" "secret volume uses the chart Secret"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "secrets") | .secret.defaultMode')" "288" "secret defaultMode 0440"
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts[] | select(.mountPath == "/run/secrets/darling") | .readOnly')" "true" "secret mount read-only"
render --set secrets.values.web-token=given
assert_eq "$(doc Secret "${DEPLOY}" | yq '.data.web-token | @base64d')" "given" "given key wins over generation"

t init_container_substitutes
render --set 'configSubstitutions[0].placeholder=@@HOOK@@' --set 'configSubstitutions[0].secretKey=hook'
assert_eq "$(dq '.spec.template.spec.initContainers[0].name')" '"render-config"' "init container name"
assert_eq "$(dq '.spec.template.spec.initContainers[0].env[] | select(.name == "DARLING_SUBST_0_PLACEHOLDER") | .value')" '"@@HOOK@@"' "placeholder env"
assert_eq "$(dq '.spec.template.spec.initContainers[0].env[] | select(.name == "DARLING_SUBST_0_VALUE") | .valueFrom.secretKeyRef.key')" '"hook"' "value from the chart Secret"

# NOTES.txt is `include "performancemonitor-darling.notes"`, and `helm template` never prints NOTES.txt while
# `helm install --dry-run=client` still reaches for a cluster. So the tests render the same helper through a probe template
# added to a throwaway copy of the chart: no cluster, no kubeconfig.
notes() {
  local copy
  copy="$(mktemp -d)"
  cp -r "${CHART_DIR}/." "${copy}/"
  cat > "${copy}/templates/notes-probe.yaml" <<'PROBE'
apiVersion: v1
kind: ConfigMap
metadata:
  name: notes
data:
  text: |
{{ include "performancemonitor-darling.notes" . | indent 4 }}
PROBE
  helm template "${RELEASE}" "${copy}" --show-only templates/notes-probe.yaml "$@" | yq '.data.text'
  rm -rf "${copy}"
}

t notes_warn_generated_secrets
n="$(notes)"
grep -qF existingSecret <<<"${n}" || { echo "NOTES lack existingSecret" >&2; exit 1; }
grep -qF GitOps <<<"${n}" || { echo "NOTES lack GitOps" >&2; exit 1; }
n="$(notes --set secrets.existingSecret=s --set store.bundled.auth.existingSecret=x)"
if grep -qF existingSecret <<<"${n}" || grep -qF GitOps <<<"${n}"; then echo "NOTES warn with an existing Secret" >&2; exit 1; fi

t notes_say_darling_json_is_a_seed
for args in "" "--set secrets.existingSecret=s --set store.bundled.auth.existingSecret=x --set image.tag=1"; do
  # shellcheck disable=SC2086
  n="$(notes ${args})"
  grep -qF 'darling.json is only a seed' <<<"${n}" || { echo "NOTES do not say darling.json is a seed (${args:-defaults})" >&2; exit 1; }
done

t image_tag_minimum
for tag in 3.12.0 v3.12.0 3.11.2; do
  assert_fails "needs a Darling image newer than 3.12.0" --set-string "image.tag=${tag}"
done
for tag in 3.13.0 v3.13.0 latest pr dev abc1234; do
  render --set-string "image.tag=${tag}"
done

t deployment_annotations
render
assert_eq "$(dq '.metadata.annotations')" "null" "no deployment annotations by default"
render --set-json 'deploymentAnnotations={"reloader.example/auto":"true"}'
assert_eq "$(dq '.metadata.annotations["reloader.example/auto"]')" '"true"' "deploymentAnnotations"
assert_eq "$(dq '.spec.template.metadata.annotations["reloader.example/auto"]')" "null" "not on the pod"

t notes_warn_latest_tag
n="$(notes)"
grep -qF 'moves' <<<"${n}" || { echo "NOTES lack the moving-tag warning for the latest tag" >&2; exit 1; }
n="$(notes --set image.tag=3.13.0)"
if grep -qF 'moves' <<<"${n}"; then echo "NOTES warn about a pinned tag" >&2; exit 1; fi
n="$(notes)"
grep -qF 'newer than 3.12.0' <<<"${n}" || { echo "NOTES lack the minimum-image caveat for the latest tag" >&2; exit 1; }

t fail_owned_key_in_config_any_case
assert_fails "config.postgres.connectionString is owned by the chart" --set config.Postgres.connectionString=x
assert_fails "config.postgres.connectionString is owned by the chart" --set config.POSTGRES.CONNECTIONSTRING=x
assert_fails "config.web.network.token is owned by the chart" --set config.web.Network.token=x
assert_fails "config.web.port is owned by the chart" --set config.Web.Port=1
assert_fails "config.health is owned by the chart" --set config.HEALTH.port=1

# The bundled store.
SS="${RELEASE}-performancemonitor-darling-store"
ssq() {
  doc StatefulSet "${SS}" | yq -o=json -I=0 "$1"
}

t bundled_default
render
assert_eq "$(ssq '.spec.replicas')" "1" "store replicas"
assert_eq "$(ssq '.spec.serviceName')" "\"${SS}-headless\"" "store serviceName"
assert_eq "$(ssq '.spec.template.spec.containers[0].image')" '"timescale/timescaledb:2.31.0-pg17"' "store image"
assert_eq "$(ssq '.spec.template.spec.containers[0].env[] | select(.name == "PGDATA") | .value')" '"/var/lib/postgresql/data/pgdata"' "PGDATA"
assert_eq "$(ssq '.spec.template.spec.containers[0].volumeMounts[] | select(.name == "data") | .mountPath')" '"/var/lib/postgresql/data"' "data mount"
assert_eq "$(ssq '.spec.template.spec.containers[0].env[] | select(.name == "POSTGRES_USER") | .value')" '"darling"' "POSTGRES_USER"
assert_eq "$(ssq '.spec.template.spec.containers[0].env[] | select(.name == "POSTGRES_DB") | .value')" '"darling"' "POSTGRES_DB"
assert_eq "$(ssq '.spec.template.spec.containers[0].env[] | select(.name == "POSTGRES_PASSWORD_FILE") | .value')" '"/run/secrets/store/password"' "POSTGRES_PASSWORD_FILE"
assert_eq "$(ssq '.spec.template.spec.volumes[] | select(.name == "store-password") | .secret.secretName')" "\"${SS}\"" "store password volume"
assert_eq "$(ssq '.spec.template.spec.volumes[] | select(.name == "store-password") | .secret.items[0].key')" '"password"' "store password key"
assert_eq "$(ssq '.spec.template.spec.containers[0].readinessProbe.exec.command | join(" ")')" '"pg_isready -U darling -d darling"' "store readiness"
assert_eq "$(ssq '.spec.template.spec.containers[0].resources')" '{"limits":{"memory":"4Gi"},"requests":{"memory":"2Gi"}}' "store resources"
assert_eq "$(ssq '.spec.volumeClaimTemplates[0].spec.resources.requests.storage')" '"10Gi"' "store pvc size"
# 70:70 is the postgres user of the store image: docker run --rm --entrypoint id timescale/timescaledb:2.31.0-pg17 postgres
assert_eq "$(ssq '.spec.template.spec.securityContext')" \
  '{"runAsNonRoot":true,"runAsUser":70,"runAsGroup":70,"fsGroup":70,"fsGroupChangePolicy":"OnRootMismatch","seccompProfile":{"type":"RuntimeDefault"}}' "store pod securityContext"
assert_eq "$(ssq '.spec.template.spec.containers[0].securityContext')" \
  '{"allowPrivilegeEscalation":false,"readOnlyRootFilesystem":true,"capabilities":{"drop":["ALL"]}}' "store container securityContext"
assert_eq "$(ssq '.spec.template.spec.automountServiceAccountToken')" "false" "store automountServiceAccountToken"
assert_eq "$(ssq '.spec.template.spec.enableServiceLinks')" "false" "store enableServiceLinks"
assert_eq "$(doc Secret "${SS}" | yq '.data | has("password")')" "true" "generated store password"
assert_eq "$(doc Secret "${SS}" | yq '.metadata.annotations["helm.sh/resource-policy"]')" "keep" "store secret keep annotation"
assert_eq "$(doc Service "${SS}" | yq '.spec.type')" "ClusterIP" "store Service type"
assert_eq "$(doc Service "${SS}-headless" | yq '.spec.clusterIP')" "None" "headless Service"
# The store pod is never selected by the Darling Deployment.
assert_eq "$(ssq '.spec.template.metadata.labels["app.kubernetes.io/name"]')" '"performancemonitor-darling-store"' "store name label"
# Darling reaches the store through the inline string; the password comes from the store Secret by env.
assert_eq "$(config_json '.postgres.connectionString')" "\"Host=${SS};Port=5432;Username=darling;Database=darling\"" "postgres.connectionString"
assert_eq "$(config_json '.postgres.managed')" "false" "postgres.managed"
assert_eq "$(dq '.spec.template.spec.containers[0].env[] | select(.name == "PGPASSWORD") | .valueFrom.secretKeyRef')" "{\"name\":\"${SS}\",\"key\":\"password\"}" "PGPASSWORD from the store Secret"
assert_eq "$(doc Secret "${DEPLOY}" | yq '.data | has("store-connection")')" "false" "no store-connection in the chart Secret"

t bundled_existing_secret
render --set store.bundled.auth.existingSecret=x
assert_eq "$(yq "select(.kind == \"Secret\" and .metadata.name == \"${SS}\") | .metadata.name" "${OUT}")" "" "no store Secret rendered"
assert_eq "$(ssq '.spec.template.spec.volumes[] | select(.name == "store-password") | .secret.secretName')" '"x"' "store pod reads x"
assert_eq "$(dq '.spec.template.spec.containers[0].env[] | select(.name == "PGPASSWORD") | .valueFrom.secretKeyRef.name')" '"x"' "Darling pod reads x"

t store_custom_values
render --set store.bundled.persistence.size=50Gi,store.bundled.persistence.storageClass=fast,store.bundled.image=example/pg:1
assert_eq "$(ssq '.spec.volumeClaimTemplates[0].spec.resources.requests.storage')" '"50Gi"' "custom size"
assert_eq "$(ssq '.spec.volumeClaimTemplates[0].spec.storageClassName')" '"fast"' "custom storage class"
assert_eq "$(ssq '.spec.template.spec.containers[0].image')" '"example/pg:1"' "custom image"

EXT=(--set store.mode=external --set store.external.host=db.example --set store.external.database=darling --set store.external.username=app)

t external_renders_no_statefulset
render "${EXT[@]}"
assert_eq "$(yq 'select(.kind == "StatefulSet") | .metadata.name' "${OUT}")" "" "no StatefulSet"
assert_eq "$(yq 'select(.kind == "Service" and (.metadata.name | test("-store"))) | .metadata.name' "${OUT}")" "" "no store Services"
assert_eq "$(yq "select(.kind == \"Secret\" and .metadata.name == \"${SS}\") | .metadata.name" "${OUT}")" "" "no store Secret"

t external_pgpassword
render "${EXT[@]}" --set store.external.passwordSecret.name=pg --set store.external.passwordSecret.key=pw --set store.external.sslMode=Require
assert_eq "$(config_json '.postgres.connectionString')" '"Host=db.example;Port=5432;Database=darling;Username=app;SSL Mode=Require"' "connection string"
assert_eq "$(dq '.spec.template.spec.containers[0].env[] | select(.name == "PGPASSWORD") | .valueFrom.secretKeyRef')" '{"name":"pg","key":"pw"}' "PGPASSWORD from passwordSecret"
assert_eq "$(config_json '.postgres.connectionString | contains("Password")')" "false" "no Password in the string"

t external_connection_string_secret
render --set store.mode=external --set store.external.connectionStringSecret.name=cs --set store.external.connectionStringSecret.key=conn
assert_eq "$(config_json '.postgres.connectionString')" '"file:/run/secrets/darling-store/conn"' "file: connection string"
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts[] | select(.mountPath == "/run/secrets/darling-store")')" '{"name":"store-connection","mountPath":"/run/secrets/darling-store","readOnly":true}' "connection mount"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "store-connection") | .secret')" '{"secretName":"cs","defaultMode":288,"items":[{"key":"conn","path":"conn"}]}' "connection volume"
assert_eq "$(dq '[.spec.template.spec.containers[0].env[] | select(.name == "PGPASSWORD")] | length')" "0" "no PGPASSWORD"

t external_ca
render "${EXT[@]}" --set store.external.caSecret.name=ca --set store.external.caSecret.key=root.pem
assert_eq "$(config_json '.postgres.connectionString')" '"Host=db.example;Port=5432;Database=darling;Username=app;SSL Mode=Prefer;Root Certificate=/etc/darling/store-ca/ca.crt"' "connection string with CA"
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts[] | select(.mountPath == "/etc/darling/store-ca")')" '{"name":"store-ca","mountPath":"/etc/darling/store-ca","readOnly":true}' "CA mount"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "store-ca") | .secret')" '{"secretName":"ca","defaultMode":288,"items":[{"key":"root.pem","path":"ca.crt"}]}' "CA volume"

t external_no_auth_allowed
render "${EXT[@]}"
assert_eq "$(dq '[.spec.template.spec.containers[0].env[] | select(.name == "PGPASSWORD")] | length')" "0" "no PGPASSWORD without a secret"

t external_notes_no_store_warning
n="$(notes --set secrets.existingSecret=s "${EXT[@]}" --set image.tag=1)"
if grep -qE "store's password|bundled store" <<<"${n}"; then echo "NOTES mention a store password in external mode" >&2; exit 1; fi

t schema_rejects
assert_fails "store.mode" --set store.mode=bogus
assert_fails "host" --set store.mode=external --set store.external.database=d --set store.external.username=u
assert_fails "store.external" "${EXT[@]}" --set store.external.passwordSecret.name=a --set store.external.connectionStringSecret.name=b
assert_fails "store.external.host" "${EXT[@]}" --set 'store.external.host=a;Password=x'
assert_fails "store.external.database" "${EXT[@]}" --set 'store.external.database=a=b'
assert_fails "store.external.username" "${EXT[@]}" --set 'store.external.username=a;b'

t notes_warn_generated_store_password
n="$(notes --set secrets.existingSecret=s)"
grep -qF "bundled store's password" <<<"${n}" || { echo "NOTES do not warn about the generated store password" >&2; exit 1; }
grep -qF 'GitOps' <<<"${n}" || { echo "NOTES lack GitOps for the generated store password" >&2; exit 1; }
n="$(notes --set secrets.existingSecret=s --set store.bundled.auth.existingSecret=x --set image.tag=1)"
if grep -qF 'generated' <<<"${n}"; then echo "NOTES warn with both Secrets given" >&2; exit 1; fi

# ---- Task 5: network ----

t services_per_surface
render
WS="${DEPLOY}-web"
MS="${DEPLOY}-mcp"
assert_eq "$(doc Service "${WS}" | yq -o=json -I=0 '.spec.ports | map(.port)')" "[5153]" "web Service port"
assert_eq "$(yq 'select(.kind == "Service" and .metadata.name == "'"${MS}"'") | .metadata.name' "${OUT}")" "" "no MCP Service by default"
render --set mcp.enabled=true
assert_eq "$(doc Service "${MS}" | yq -o=json -I=0 '.spec.ports | map(.port)')" "[5152]" "MCP Service port"
assert_eq "$(yq 'select(.kind == "Service") | .spec.ports[].port' "${OUT}" | grep -c '^5154$' || true)" "0" "no Service for 5154"
assert_eq "$(doc Service "${WS}" | yq '.spec.selector."app.kubernetes.io/name"')" "performancemonitor-darling" "web Service selects the Darling pod"
render --set web.enabled=false
assert_eq "$(yq 'select(.kind == "Service" and .metadata.name == "'"${WS}"'") | .metadata.name' "${OUT}")" "" "no web Service when web is off"

t default_exposure_loopback_only
render
assert_eq "$(config_json '.web.network.listen')" '"0.0.0.0"' "listen"
assert_eq "$(config_json '.web.network.allowFrom')" '["127.0.0.1/32"]' "allowFrom admits loopback only"
assert_eq "$(config_json '.web.network.token')" '"file:/run/secrets/darling/web-token"' "web token"
assert_eq "$(config_json '.web.enabled')" "true" "web enabled"
assert_eq "$(config_json '.web.port')" "5153" "web port"
assert_eq "$(config_json '.mcp.enabled')" "false" "mcp off"
assert_eq "$(config_json '.mcp.network')" "null" "no mcp network block while off"
assert_eq "$(config_json '.web | has("publicBaseUrl")')" "false" "no publicBaseUrl"
assert_eq "$(config_json '.web.network | has("trustedProxies")')" "false" "no trustedProxies"
render --set mcp.enabled=true --set network.allowFrom='{10.0.0.0/8}' --set mcp.network.allowFrom='{192.168.0.0/16}'
assert_eq "$(config_json '.web.network.allowFrom')" '["10.0.0.0/8"]' "shared allowFrom"
assert_eq "$(config_json '.mcp.network.allowFrom')" '["192.168.0.0/16"]' "mcp override"
assert_eq "$(config_json '.mcp.network.token')" '"file:/run/secrets/darling/mcp-token"' "mcp token"
assert_eq "$(config_json '.mcp.network.listen')" '"0.0.0.0"' "mcp listen"

t public_base_url_default
ING=(--set web.ingress.enabled=true --set 'web.ingress.hosts={d.example.com}' --set 'network.trustedProxies={10.1.0.0/16}' --set 'network.allowFrom={10.1.0.0/16}')
render "${ING[@]}" --set 'web.ingress.tls[0].secretName=d-tls' --set 'web.ingress.tls[0].hosts={d.example.com}'
assert_eq "$(config_json '.web.publicBaseUrl')" '"https://d.example.com"' "https from tls"
render "${ING[@]}"
assert_eq "$(config_json '.web.publicBaseUrl')" '"http://d.example.com"' "http without tls"
render "${ING[@]}" --set 'web.ingress.tls[0].secretName=d-tls' --set 'web.ingress.tls[0].hosts={d.example.com}' --set web.publicBaseUrl=https://override.example.com
assert_eq "$(config_json '.web.publicBaseUrl')" '"https://override.example.com"' "explicit publicBaseUrl wins"
assert_eq "$(config_json '.web.network.trustedProxies')" '["10.1.0.0/16"]' "trustedProxies"
assert_eq "$(doc Ingress "${DEPLOY}-web" | yq -o=json -I=0 '.spec.rules[0].http.paths[0]')" \
  '{"path":"/","pathType":"Prefix","backend":{"service":{"name":"'"${DEPLOY}-web"'","port":{"number":5153}}}}' "ingress backend"

t mcp_ingress
render --set mcp.enabled=true --set mcp.ingress.enabled=true --set 'mcp.ingress.hosts={m.example.com}' --set 'network.allowFrom={10.1.0.0/16}' --set mcp.ingress.className=example-ingress
assert_eq "$(doc Ingress "${DEPLOY}-mcp" | yq '.spec.ingressClassName')" "example-ingress" "mcp class"
assert_eq "$(config_json '.mcp.network.hostName')" '"m.example.com"' "mcp hostName from the Ingress host"

t fail_ingress_without_trusted_proxies
assert_fails "network.trustedProxies" --set web.ingress.enabled=true --set 'web.ingress.hosts={d.example.com}' --set 'network.allowFrom={10.1.0.0/16}'

t fail_ingress_without_allow_from
assert_fails "network.allowFrom" --set web.ingress.enabled=true --set 'web.ingress.hosts={d.example.com}' --set 'network.trustedProxies={10.1.0.0/16}'
assert_fails "network.allowFrom" --set mcp.enabled=true --set mcp.ingress.enabled=true --set 'mcp.ingress.hosts={m.example.com}'

t fail_ingress_allow_from_names_what_each_surface_judges
# The dashboard judges the forwarded client (operators), the MCP endpoint the connection (the controller pods).
assert_fails "web.network.allowFrom (or network.allowFrom): the client addresses the ingress controller forwards" \
  --set web.ingress.enabled=true --set 'web.ingress.hosts={d.example.com}' --set 'network.trustedProxies={10.1.0.0/16}'
assert_fails "mcp.network.allowFrom (or network.allowFrom): the CIDRs of the ingress controller pods" \
  --set mcp.enabled=true --set mcp.ingress.enabled=true --set 'mcp.ingress.hosts={m.example.com}'
assert_fails "X-Forwarded-For" --set web.ingress.enabled=true --set 'web.ingress.hosts={d.example.com}' --set 'network.allowFrom={10.1.0.0/16}'
if helm template "${RELEASE}" "${CHART_DIR}" --set web.ingress.enabled=true --set 'web.ingress.hosts={d.example.com}' \
  --set 'network.allowFrom={10.1.0.0/16}' 2>&1 | grep -qF "refuses every request"; then
  echo "the trustedProxies message still says the dashboard refuses every request" >&2; exit 1
fi

t owned_key_messages
assert_fails "config.postgres.managed is owned by the chart and cannot be set inside config: the chart sets it, and it cannot be overridden" --set config.postgres.managed=true
assert_fails "config.health is owned by the chart and cannot be set inside config: the chart sets it, and it cannot be overridden" --set config.health.port=1
if helm template "${RELEASE}" "${CHART_DIR}" --set config.postgres.managed=true 2>&1 | grep -qF "use the value nothing"; then
  echo "the owned-key message still says 'use the value nothing'" >&2; exit 1
fi
# Darling has no mcp.network.trustedProxies, so the chart does not own (or refuse) it.
render --set mcp.enabled=true --set 'config.mcp.network.trustedProxies={10.0.0.0/8}'

t fail_ingress_misconfigured
assert_fails "web.ingress.hosts" --set web.ingress.enabled=true --set 'network.trustedProxies={10.1.0.0/16}' --set 'network.allowFrom={10.1.0.0/16}'
assert_fails "mcp.enabled" --set mcp.ingress.enabled=true --set 'mcp.ingress.hosts={m.example.com}' --set 'network.allowFrom={10.1.0.0/16}'
assert_fails "web.port" --set web.port=0

# ---- Task 6: passwordKey ----
t password_key_unset_by_default
render
assert_eq "$(config_json 'has("passwordKey")')" "false" "no passwordKey in the config"
assert_eq "$(dq '.spec.template.spec.volumes | map(select(.name == "password-key")) | length')" "0" "no password-key volume"
assert_eq "$(dq '.spec.template.spec.containers[0].env | map(select(.name == "DARLING_PASSWORD_KEY")) | length')" "0" "no password key env"

t password_key_file
render --set passwordKey.existingSecret=pk
assert_eq "$(config_json '.passwordKey')" '"file:/run/secrets/darling-password-key/password-key"' "passwordKey file reference"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "password-key") | .secret')" '{"secretName":"pk","defaultMode":288,"items":[{"key":"password-key","path":"password-key"}]}' "password-key volume"
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts[] | select(.name == "password-key")')" '{"name":"password-key","mountPath":"/run/secrets/darling-password-key","readOnly":true}' "password-key mount"
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts | map(select(.mountPath | contains("/var/lib/darling/credentials"))) | length')" "0" "nothing mounted inside credentials/"
render --set passwordKey.existingSecret=pk --set passwordKey.secretKey=k2
assert_eq "$(config_json '.passwordKey')" '"file:/run/secrets/darling-password-key/k2"' "custom key name"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "password-key") | .secret.items')" '[{"key":"k2","path":"k2"}]' "custom key item"

t password_key_env
render --set passwordKey.existingSecret=pk --set passwordKey.source=env
assert_eq "$(config_json '.passwordKey')" '"env:DARLING_PASSWORD_KEY"' "passwordKey env reference"
assert_eq "$(dq '.spec.template.spec.containers[0].env[] | select(.name == "DARLING_PASSWORD_KEY") | .valueFrom.secretKeyRef')" '{"name":"pk","key":"password-key"}' "env from secretKeyRef"
assert_eq "$(dq '.spec.template.spec.volumes | map(select(.name == "password-key")) | length')" "0" "no password-key volume"
assert_eq "$(dq '.spec.template.spec.containers[0].volumeMounts | map(select(.name == "password-key")) | length')" "0" "no password-key mount"

t password_key_never_group_writable
assert_fails "defaultMode" --set passwordKey.existingSecret=pk --set passwordKey.defaultMode=432
assert_fails "source" --set passwordKey.existingSecret=pk --set passwordKey.source=bogus
render --set passwordKey.existingSecret=pk
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "password-key") | .secret.defaultMode')" "288" "mode stays 0440"

t probes_health_port
render
assert_eq "$(config_json '.health')" '{"enabled":true,"listen":"0.0.0.0","port":5154}' "rendered health block"
assert_eq "$(dq '.spec.template.spec.containers[0].startupProbe')" '{"httpGet":{"path":"/livez","port":"health"},"periodSeconds":10,"failureThreshold":30,"timeoutSeconds":5}' "startupProbe"
assert_eq "$(dq '.spec.template.spec.containers[0].livenessProbe')" '{"httpGet":{"path":"/livez","port":"health"},"periodSeconds":20,"failureThreshold":3,"timeoutSeconds":5}' "livenessProbe"
assert_eq "$(dq '.spec.template.spec.containers[0].readinessProbe')" '{"httpGet":{"path":"/readyz","port":"health"},"periodSeconds":10,"failureThreshold":3,"timeoutSeconds":5}' "readinessProbe"
assert_eq "$(dq '.spec.template.spec.containers[0].ports')" '[{"name":"web","containerPort":5153,"protocol":"TCP"},{"name":"health","containerPort":5154,"protocol":"TCP"}]' "ports, mcp off"
assert_eq "$(dq '[.spec.template.spec.containers[0] | (.startupProbe, .livenessProbe, .readinessProbe) | .httpGet.port] | unique')" '["health"]' "no probe targets web, mcp or a number"
render --set mcp.enabled=true --set web.enabled=false
assert_eq "$(dq '.spec.template.spec.containers[0].ports | map(.name)')" '["mcp","health"]' "ports, web off, mcp on"
assert_fails "config.health is owned by the chart" --set config.health.port=9999

t fail_port_clashes_with_health
assert_fails "5154" --set web.port=5154
assert_fails "5154" --set mcp.enabled=true --set mcp.port=5154

t acceptance_values_render
for f in "${CHART_DIR}"/ci/*.yaml; do
  helm lint "${CHART_DIR}" -f "${f}" > /dev/null
  render -f "${f}"
done
render -f "${CHART_DIR}/ci/external-cnpg-style.yaml"
assert_eq "$(dq '.spec.template.spec.hostAliases | length')" "2" "hostAliases"
assert_eq "$(dq '.spec.template.spec.containers[0].resources.limits.memory')" '"4Gi"' "4Gi limit"
assert_eq "$(dq '.spec.template.metadata.annotations."example.com/scrape"')" '"true"' "pod annotation"
assert_eq "$(dq '.spec.template.spec.volumes[] | select(.name == "state") | .persistentVolumeClaim.claimName')" '"darling-state"' "existing claim"
assert_eq "$(config_json '.mcp.tokens | length')" "2" "two MCP tokens"
assert_eq "$(config_json '.postgres.connectionString')" '"Host=sql.example.internal;Port=5432;Database=darling;Username=darling;SSL Mode=VerifyFull;Root Certificate=/etc/darling/store-ca/ca.crt"' "external connection string"
assert_eq "$(yq 'select(.kind == "Ingress") | .metadata.name' "${OUT}" | grep -v -- '---' | sort | tr '\n' ' ')" "t-performancemonitor-darling-mcp t-performancemonitor-darling-web " "both Ingresses"
assert_eq "$(config_json '.web.network.allowFrom')" '["192.0.2.0/24"]' "web allowFrom: the operators' network"
assert_eq "$(config_json '.web.network.trustedProxies')" '["10.244.0.0/16"]' "web trustedProxies: the controller pods"
assert_eq "$(config_json '.mcp.network.allowFrom')" '["10.244.0.0/16"]' "mcp allowFrom: the controller pods"

t names_fit_with_the_longest_release_name
# 53 characters, Helm's longest release name. Every object name must stay a valid label value (63), and the store
# StatefulSet at most 52, because its pods get a controller-revision-hash label of the name plus 11 characters.
LONG="$(printf 'r%.0s' $(seq 1 53))"
helm template "${LONG}" "${CHART_DIR}" --set mcp.enabled=true --set mcp.ingress.enabled=true --set 'mcp.ingress.hosts={m.example.com}' \
  --set web.ingress.enabled=true --set 'web.ingress.hosts={d.example.com}' --set 'network.allowFrom={10.1.0.0/16}' \
  --set 'network.trustedProxies={10.1.0.0/16}' > "${OUT}"
assert_eq "$(yq '.metadata.name | select(length > 63)' "${OUT}" | grep -v -- '---' || true)" "" "names over 63 characters"
assert_eq "$(yq 'select(.kind == "StatefulSet") | .metadata.name | length' "${OUT}")" "52" "StatefulSet name length"
assert_eq "$(yq 'select(.kind == "StatefulSet") | .spec.serviceName | length <= 63' "${OUT}")" "true" "serviceName fits"
assert_eq "$(yq 'select(.kind == "StatefulSet") | ("data-" + .metadata.name + "-0") | length <= 63' "${OUT}")" "true" "store PVC name fits"
assert_eq "$(yq '[.. | select(has("app.kubernetes.io/name")) | .["app.kubernetes.io/name"] | select(length > 63)] | length' "${OUT}" | grep -v -- '---' | sort -u)" "0" "name labels fit"
# Every Service and Ingress name is distinct, and each Ingress backend names a rendered Service.
assert_eq "$(yq 'select(.kind == "Service" or .kind == "Ingress") | .kind + "/" + .metadata.name' "${OUT}" | grep -v -- '---' | sort | uniq -d)" "" "duplicate names"
for b in $(yq 'select(.kind == "Ingress") | .spec.rules[].http.paths[].backend.service.name' "${OUT}" | grep -v -- '---'); do
  assert_eq "$(yq "select(.kind == \"Service\" and .metadata.name == \"${b}\") | .metadata.name" "${OUT}")" "${b}" "Ingress backend ${b} is a Service"
done
assert_eq "$(yq 'select(.kind == "ConfigMap") | .data["darling.json"]' "${OUT}" | yq -p json '.postgres.connectionString' | sed -E 's/^Host=([^;]*);.*/\1/')" \
  "$(yq 'select(.kind == "Service" and .spec.clusterIP != "None" and (.metadata.name | test("-store$"))) | .metadata.name' "${OUT}" | grep -v -- '---')" "Darling connects to the store Service"

t state_pvc_kept_on_uninstall
render
assert_eq "$(doc PersistentVolumeClaim "${DEPLOY}" | yq '.metadata.annotations["helm.sh/resource-policy"]')" "keep" "state PVC keep annotation"

t fail_top_level_key_in_another_case
# The chart merges its own postgres/web/mcp/health/passwordKey blocks; a differently-cased copy in config would sit beside
# them in darling.json, and Darling (case-insensitive) would read only one of the two.
assert_fails "config.Postgres must be spelled config.postgres" --set config.Postgres.webConnectionString=x
assert_fails "config.WEB must be spelled config.web" --set config.WEB.network.oidc.authority=https://idp.example.com
assert_fails "config.Mcp must be spelled config.mcp" --set config.Mcp.tokens=x
# health and passwordKey are owned whole, so any spelling fails as owned.
assert_fails "config.passwordKey is owned by the chart" --set config.PasswordKey=x
render --set config.postgres.webConnectionString=x
assert_eq "$(config_json '.postgres.webConnectionString')" '"x"' "exact spelling passes through"

t init_container_resources
render
assert_eq "$(dq '.spec.template.spec.initContainers[0].resources')" '{"requests":{"cpu":"10m","memory":"16Mi"},"limits":{"memory":"64Mi"}}' "init container resources"

t store_termination_grace
render
assert_eq "$(ssq '.spec.template.spec.terminationGracePeriodSeconds')" "120" "store terminationGracePeriodSeconds"

t notes_token_command_for_existing_secret
n="$(notes --set secrets.existingSecret=mine --set store.bundled.auth.existingSecret=x --set image.tag=1)"
grep -qF "kubectl get secret mine" <<<"${n}" || { echo "NOTES lack the token command for an existingSecret" >&2; exit 1; }
grep -qF "{.data.web-token}" <<<"${n}" || { echo "NOTES do not name the web token key" >&2; exit 1; }
n="$(notes --set secrets.existingSecret=mine --set secrets.webTokenKey=dash --set store.bundled.auth.existingSecret=x --set image.tag=1)"
grep -qF "{.data.dash}" <<<"${n}" || { echo "NOTES do not name a custom web token key" >&2; exit 1; }

t fail_network_block_in_another_case
# The chart writes its own network block inside web and mcp; a Network block beside it would lose its tls or oidc settings.
assert_fails "config.web.Network must be spelled config.web.network" --set config.web.Network.tls.certPath=cert.pem
assert_fails "config.mcp.NETWORK must be spelled config.mcp.network" --set config.mcp.NETWORK.tls.certPath=cert.pem
render --set config.web.network.tls.certPath=cert.pem
assert_eq "$(config_json '.web.network.tls.certPath')" '"cert.pem"' "web.network.tls passes through"
assert_eq "$(config_json '.web.network.token')" '"file:/run/secrets/darling/web-token"' "chart network keys kept beside it"

t placeholder_must_be_at_tokens
# A placeholder such as "darling" or "token" would rewrite chart-owned values (file:/run/secrets/darling/web-token).
for p in darling token '@@a-b@@' '@@@@' 'x@@A@@'; do
  assert_fails "Does not match pattern" --set "configSubstitutions[0].placeholder=${p}" --set 'configSubstitutions[0].secretKey=k'
done
render --set 'configSubstitutions[0].placeholder=@@Hook_1@@' --set 'configSubstitutions[0].secretKey=k'

# The keys of the secrets volume's items, comma-joined; empty when the volume is not rendered.
secret_items() {
  dq '[.spec.template.spec.volumes[] | select(.name == "secrets") | .secret.items[] | .key + "=" + .path] | join(",")' | tr -d '"'
}

t secret_mount_only_referenced_keys
# Only the keys darling.json references as file:/run/secrets/darling/<key> are mounted: a substitution-only key reaches the
# init container through env and is never a file a server's password reference could name.
render --set mcp.enabled=true \
  --set-json 'config={"servers":[{"name":"a","host":"h","password":"file:/run/secrets/darling/sql-a"}],"webhooks":{"slackUrl":"@@HOOK@@"}}' \
  --set 'configSubstitutions[0].placeholder=@@HOOK@@' --set 'configSubstitutions[0].secretKey=hook'
assert_eq "$(secret_items)" "mcp-token=mcp-token,sql-a=sql-a,web-token=web-token" "mounted keys"
assert_eq "$(dq '.spec.template.spec.initContainers[0].env[] | select(.name == "DARLING_SUBST_0_VALUE") | .valueFrom.secretKeyRef.key')" '"hook"' "hook still reaches the init container"
# Only a whole string value that starts with file: counts, as Darling's own reference check reads it (an ordinal prefix,
# the path trimmed): text before file:, a leading space, a deeper path, an escaped quote or an object key spelled like a
# reference is not mounted; whitespace around the path and a value inside a list are.
render --set-json 'config={"notes":"see xfile:/run/secrets/darling/zz","a":" file:/run/secrets/darling/lead","b":"file:/run/secrets/darling/sub/x","file:/run/secrets/darling/keyname":"v","e":"a\"file:/run/secrets/darling/esc\"","c":"file: /run/secrets/darling/sp ","d":["file:/run/secrets/darling/arr"]}'
assert_eq "$(secret_items)" "arr=arr,sp=sp,web-token=web-token" "only whole file: values"
render
assert_eq "$(secret_items)" "web-token=web-token" "default: the web token only"
render --set secrets.webTokenKey=dash
assert_eq "$(secret_items)" "dash=dash" "custom token key"
# Other mounts under /run/secrets are not the chart Secret's keys.
render --set passwordKey.existingSecret=pk
assert_eq "$(secret_items)" "web-token=web-token" "password key not in the chart Secret's items"
# Nothing referenced: no secrets volume and no mount at all, never an empty items list.
render --set web.enabled=false --set 'configSubstitutions[0].placeholder=@@HOOK@@' --set 'configSubstitutions[0].secretKey=hook' \
  --set-json 'config={"webhooks":{"slackUrl":"@@HOOK@@"}}'
assert_eq "$(dq '[.spec.template.spec.volumes[] | select(.name == "secrets")] | length')" "0" "no secrets volume"
assert_eq "$(dq '[.spec.template.spec.containers[0].volumeMounts[] | select(.mountPath == "/run/secrets/darling")] | length')" "0" "no secrets mount"
