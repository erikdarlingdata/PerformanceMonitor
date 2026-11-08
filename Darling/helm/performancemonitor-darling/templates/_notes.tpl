{{/* The text of NOTES.txt. It lives in a helper so the render tests can render it without a cluster. */}}
{{- define "performancemonitor-darling.notes" -}}
Darling is installed as {{ include "performancemonitor-darling.fullname" . }}.

darling.json is only a seed for servers, alerts, webhooks and the other first-run settings: after the first start the
store keeps them, and a change to `config` no longer reaches them. Change them in the Viewer, the dashboard or the MCP
tools (the chart README lists which keys are seeds).
{{- if or (not .Values.secrets.existingSecret) (include "performancemonitor-darling.storePasswordGenerated" .) }}

WARNING: generated values are in use:
{{- if not .Values.secrets.existingSecret }}
- the web and MCP tokens in the Secret {{ include "performancemonitor-darling.secretName" . }}
{{- end }}
{{- if include "performancemonitor-darling.storePasswordGenerated" . }}
- the bundled store's password in the Secret {{ include "performancemonitor-darling.storeSecretName" . }}
{{- end }}
They are kept across upgrades only while the Secret exists in the cluster. A tool that renders the chart without
reading the cluster (a GitOps controller, helm template) generates different values on every render. For those installs
create the Secrets yourself and set secrets.existingSecret and store.bundled.auth.existingSecret to their names.
{{- end }}
{{- if .Values.web.enabled }}

Read the web token (it is never printed here; the key is {{ .Values.secrets.webTokenKey }}):
  kubectl get secret {{ include "performancemonitor-darling.secretName" . }} --namespace {{ .Release.Namespace }} -o jsonpath='{.data.{{ .Values.secrets.webTokenKey }}}' | base64 -d
{{- end }}
{{- if eq (toString (default .Chart.AppVersion .Values.image.tag)) "latest" }}

WARNING: the image tag is "latest", a tag that moves with every release. Set image.tag to a release version (or image.digest) so an upgrade or a pod restart cannot change the collector by itself.
{{- end }}
{{- end -}}
