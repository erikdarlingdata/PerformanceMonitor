{{/* Chart name. */}}
{{- define "performancemonitor-darling.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Full resource name prefix. */}}
{{- define "performancemonitor-darling.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Selector labels. */}}
{{- define "performancemonitor-darling.selectorLabels" -}}
app.kubernetes.io/name: {{ include "performancemonitor-darling.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Common labels. */}}
{{- define "performancemonitor-darling.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "performancemonitor-darling.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Image reference: repository:tag, or repository:tag@digest when image.digest is set. */}}
{{- define "performancemonitor-darling.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- if .Values.image.digest -}}
{{- printf "%s:%s@%s" .Values.image.repository $tag .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}
{{- end -}}

{{/* Name of the state PersistentVolumeClaim. */}}
{{- define "performancemonitor-darling.claimName" -}}
{{- default (include "performancemonitor-darling.fullname" .) .Values.persistence.existingClaim -}}
{{- end -}}

{{/* Name of the Secret the pod reads: the operator's existingSecret, or the chart-created one. */}}
{{- define "performancemonitor-darling.secretName" -}}
{{- default (include "performancemonitor-darling.fullname" .) .Values.secrets.existingSecret -}}
{{- end -}}

{{/* Prints "true" when the key path (a list of keys) exists in the map. Keys compare case-insensitively at every level,
because Darling reads darling.json case-insensitively. Arguments: dict "cfg" <map> "path" <list>. */}}
{{- define "performancemonitor-darling.pathPresent" -}}
{{- $cur := .cfg -}}
{{- $ok := true -}}
{{- range .path -}}
{{- $want := lower . -}}
{{- $next := dict -}}
{{- if and $ok (kindIs "map" $cur) -}}
{{- range $k, $v := $cur -}}
{{- if eq (lower $k) $want -}}
{{- $_ := set $next "v" $v -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if hasKey $next "v" -}}
{{- $cur = get $next "v" -}}
{{- else -}}
{{- $ok = false -}}
{{- end -}}
{{- end -}}
{{- if $ok -}}true{{- end -}}
{{- end -}}

{{/* The chart owns these keys of darling.json. Setting one inside `config` fails the render and names the value to use. */}}
{{- define "performancemonitor-darling.checkOwnedKeys" -}}
{{- $owned := list
  (list (list "postgres" "managed") "nothing: the chart sets the store mode")
  (list (list "postgres" "connectionString") "store.*")
  (list (list "web" "enabled") "web.enabled")
  (list (list "web" "port") "web.port")
  (list (list "web" "network" "listen") "web.* or network.*")
  (list (list "web" "network" "allowFrom") "web.* or network.*")
  (list (list "web" "network" "trustedProxies") "web.* or network.*")
  (list (list "web" "network" "token") "web.* or network.* (the token comes from the chart Secret)")
  (list (list "web" "publicBaseUrl") "web.publicBaseUrl")
  (list (list "mcp" "enabled") "mcp.enabled")
  (list (list "mcp" "port") "mcp.port")
  (list (list "mcp" "network" "listen") "mcp.* or network.*")
  (list (list "mcp" "network" "allowFrom") "mcp.* or network.*")
  (list (list "mcp" "network" "trustedProxies") "mcp.* or network.*")
  (list (list "mcp" "network" "token") "mcp.* or network.* (the token comes from the chart Secret)")
  (list (list "health") "nothing: the chart sets it from the probes")
  (list (list "passwordKey") "passwordKey.*")
-}}
{{- range $owned -}}
{{- $path := index . 0 -}}
{{- if include "performancemonitor-darling.pathPresent" (dict "cfg" $.Values.config "path" $path) -}}
{{- fail (printf "config.%s is owned by the chart and cannot be set inside config; use the value %s instead" (join "." $path) (index . 1)) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* darling.json: the operator's config with the chart-owned keys merged over it. */}}
{{- define "performancemonitor-darling.darlingJson" -}}
{{- include "performancemonitor-darling.checkOwnedKeys" . -}}
{{- $owned := dict "postgres" (dict "managed" false) -}}
{{- mergeOverwrite (deepCopy .Values.config) $owned | toPrettyJson -}}
{{- end -}}
