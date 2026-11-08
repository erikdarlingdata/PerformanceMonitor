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

{{/* A surface's effective network list as JSON: the per-surface override (web.network.<field>) when non-empty, else network.<field>.
Arguments: dict "ctx" <root> "surface" "web"|"mcp" "field" "allowFrom"|"trustedProxies". */}}
{{- define "performancemonitor-darling.netList" -}}
{{- $surface := get .ctx.Values .surface -}}
{{- $own := dig "network" .field (list) $surface -}}
{{- if $own -}}
{{- toJson $own -}}
{{- else -}}
{{- toJson (dig .field (list) .ctx.Values.network) -}}
{{- end -}}
{{- end -}}

{{/* Fails the render when a surface's Ingress is enabled without what makes it work. Arguments: dict "ctx" <root> "surface" "web"|"mcp". */}}
{{- define "performancemonitor-darling.checkIngress" -}}
{{- $s := .surface -}}
{{- $cfg := get .ctx.Values $s -}}
{{- if not $cfg.ingress.hosts -}}
{{- fail (printf "%s.ingress.enabled needs at least one entry in %s.ingress.hosts" $s $s) -}}
{{- end -}}
{{- if eq $s "web" -}}
{{- if not (fromJsonArray (include "performancemonitor-darling.netList" (dict "ctx" .ctx "surface" "web" "field" "trustedProxies"))) -}}
{{- fail "web.ingress.enabled needs network.trustedProxies (or web.network.trustedProxies): the CIDRs of the ingress controller pods, so the dashboard trusts the forwarded client address and scheme. Without them the dashboard refuses every request that arrives through the Ingress." -}}
{{- end -}}
{{- end -}}
{{- if not (fromJsonArray (include "performancemonitor-darling.netList" (dict "ctx" .ctx "surface" $s "field" "allowFrom"))) -}}
{{- fail (printf "%s.ingress.enabled needs network.allowFrom (or %s.network.allowFrom): the CIDRs of the ingress controller pods. Without them every request that arrives through the Ingress is refused." $s $s) -}}
{{- end -}}
{{- end -}}

{{/* The https:// or http:// URL of the web Ingress's first host: https when a tls entry covers it. */}}
{{- define "performancemonitor-darling.webIngressUrl" -}}
{{- $host := first .Values.web.ingress.hosts -}}
{{- $scheme := "http" -}}
{{- range .Values.web.ingress.tls -}}
{{- if or (not .hosts) (has $host .hosts) -}}
{{- $scheme = "https" -}}
{{- end -}}
{{- end -}}
{{- printf "%s://%s" $scheme $host -}}
{{- end -}}

{{/* The owned web or MCP block of darling.json. Arguments: dict "ctx" <root> "surface" "web"|"mcp". */}}
{{- define "performancemonitor-darling.surfaceJson" -}}
{{- $ctx := .ctx -}}
{{- $s := .surface -}}
{{- $cfg := get $ctx.Values $s -}}
{{- $block := dict "enabled" $cfg.enabled "port" (int $cfg.port) -}}
{{- if $cfg.enabled -}}
{{- if $cfg.ingress.enabled -}}
{{- include "performancemonitor-darling.checkIngress" (dict "ctx" $ctx "surface" $s) -}}
{{- end -}}
{{- $allow := fromJsonArray (include "performancemonitor-darling.netList" (dict "ctx" $ctx "surface" $s "field" "allowFrom")) -}}
{{- if not $allow -}}
{{- $allow = list "127.0.0.1/32" -}}
{{- end -}}
{{- $tokenKey := ternary $ctx.Values.secrets.webTokenKey $ctx.Values.secrets.mcpTokenKey (eq $s "web") -}}
{{- $net := dict "listen" "0.0.0.0" "allowFrom" $allow "token" (printf "file:/run/secrets/darling/%s" $tokenKey) -}}
{{- if eq $s "web" -}}
{{- $proxies := fromJsonArray (include "performancemonitor-darling.netList" (dict "ctx" $ctx "surface" "web" "field" "trustedProxies")) -}}
{{- if $proxies -}}
{{- $_ := set $net "trustedProxies" $proxies -}}
{{- end -}}
{{- if $cfg.publicBaseUrl -}}
{{- $_ := set $block "publicBaseUrl" $cfg.publicBaseUrl -}}
{{- else if $cfg.ingress.enabled -}}
{{- $_ := set $block "publicBaseUrl" (include "performancemonitor-darling.webIngressUrl" $ctx) -}}
{{- end -}}
{{- else if $cfg.ingress.enabled -}}
{{- if not (include "performancemonitor-darling.pathPresent" (dict "cfg" $ctx.Values.config "path" (list "mcp" "network" "hostName"))) -}}
{{- $_ := set $net "hostName" (first $cfg.ingress.hosts) -}}
{{- end -}}
{{- end -}}
{{- $_ := set $block "network" $net -}}
{{- else if $cfg.ingress.enabled -}}
{{- fail (printf "%s.ingress.enabled needs %s.enabled=true" $s $s) -}}
{{- end -}}
{{- toJson $block -}}
{{- end -}}

{{/* darling.json: the operator's config with the chart-owned keys merged over it. */}}
{{- define "performancemonitor-darling.darlingJson" -}}
{{- include "performancemonitor-darling.checkOwnedKeys" . -}}
{{- $postgres := dict "managed" false -}}
{{- if eq .Values.store.mode "bundled" -}}
{{- $_ := set $postgres "connectionString" (printf "Host=%s;Port=5432;Username=darling;Database=darling" (include "performancemonitor-darling.storeName" .)) -}}
{{- else -}}
{{- $ext := .Values.store.external -}}
{{- if .Values.store.external.connectionStringSecret.name -}}
{{- $_ := set $postgres "connectionString" (printf "file:/run/secrets/darling-store/%s" .Values.store.external.connectionStringSecret.key) -}}
{{- else -}}
{{- range $field := list "host" "database" "username" "sslMode" -}}
{{- if regexMatch "[;=]" (toString (get $ext $field)) -}}
{{- fail (printf "store.external.%s must not contain ';' or '='" $field) -}}
{{- end -}}
{{- end -}}
{{- $cs := printf "Host=%s;Port=%v;Database=%s;Username=%s;SSL Mode=%s" $ext.host $ext.port $ext.database $ext.username $ext.sslMode -}}
{{- if $ext.caSecret.name -}}
{{- $cs = printf "%s;Root Certificate=/etc/darling/store-ca/ca.crt" $cs -}}
{{- end -}}
{{- $_ := set $postgres "connectionString" $cs -}}
{{- end -}}
{{- end -}}
{{- range $surface := list "web" "mcp" -}}
{{- if eq (int (get (get $.Values $surface) "port")) 5154 -}}
{{- fail (printf "%s.port must not be 5154: that port is fixed for the health endpoint the probes use, and Darling refuses a health port that clashes with the web or MCP port" $surface) -}}
{{- end -}}
{{- end -}}
{{- $owned := dict "health" (dict "enabled" true "port" 5154 "listen" "0.0.0.0") "postgres" $postgres "web" (include "performancemonitor-darling.surfaceJson" (dict "ctx" . "surface" "web") | fromJson) "mcp" (include "performancemonitor-darling.surfaceJson" (dict "ctx" . "surface" "mcp") | fromJson) -}}
{{- if .Values.passwordKey.existingSecret -}}
{{- if eq .Values.passwordKey.source "env" -}}
{{- $_ := set $owned "passwordKey" "env:DARLING_PASSWORD_KEY" -}}
{{- else -}}
{{- $_ := set $owned "passwordKey" (printf "file:/run/secrets/darling-password-key/%s" .Values.passwordKey.secretKey) -}}
{{- end -}}
{{- end -}}
{{- mergeOverwrite (deepCopy .Values.config) $owned | toPrettyJson -}}
{{- end -}}

{{/* Name of the bundled store resources. */}}
{{- define "performancemonitor-darling.storeName" -}}
{{- printf "%s-store" (include "performancemonitor-darling.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Selector labels of the bundled store pod. The name label differs from the Darling pod's, so the Darling Deployment's selector never matches the store pod. */}}
{{- define "performancemonitor-darling.storeSelectorLabels" -}}
app.kubernetes.io/name: {{ include "performancemonitor-darling.name" . }}-store
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: store
{{- end -}}

{{/* Name of the Secret holding the bundled store's password (key: password): the operator's existingSecret, or the chart-created one. */}}
{{- define "performancemonitor-darling.storeSecretName" -}}
{{- default (include "performancemonitor-darling.storeName" .) .Values.store.bundled.auth.existingSecret -}}
{{- end -}}

{{/* Prints "true" when this install generates the store password: bundled store with no existingSecret. */}}
{{- define "performancemonitor-darling.storePasswordGenerated" -}}
{{- if and (eq .Values.store.mode "bundled") (not .Values.store.bundled.auth.existingSecret) -}}true{{- end -}}
{{- end -}}

{{/* The secretKeyRef of PGPASSWORD as "name|key", or nothing when the store needs no password from a Secret. */}}
{{- define "performancemonitor-darling.pgPasswordRef" -}}
{{- if eq .Values.store.mode "bundled" -}}
{{- printf "%s|password" (include "performancemonitor-darling.storeSecretName" .) -}}
{{- else if and .Values.store.external.passwordSecret.name (not .Values.store.external.connectionStringSecret.name) -}}
{{- printf "%s|%s" .Values.store.external.passwordSecret.name .Values.store.external.passwordSecret.key -}}
{{- end -}}
{{- end -}}
