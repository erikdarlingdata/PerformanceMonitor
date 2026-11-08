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
  "/config,/tmp,/usr/share/PerformanceMonitorDarling,/var/lib/darling" "mount paths"
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
