{{/* The text of NOTES.txt. It lives in a helper so the render tests can render it without a cluster. */}}
{{- define "performancemonitor-darling.notes" -}}
Darling is installed as {{ include "performancemonitor-darling.fullname" . }}.
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
{{- if not .Values.secrets.existingSecret }}

Read a token (it is never printed here):
  kubectl get secret {{ include "performancemonitor-darling.secretName" . }} --namespace {{ .Release.Namespace }} -o jsonpath='{.data.{{ .Values.secrets.webTokenKey }}}' | base64 -d
{{- end }}
{{- end }}
{{- if eq (toString (default .Chart.AppVersion .Values.image.tag)) "latest" }}

WARNING: the image tag is "latest", a tag that moves with every release. Set image.tag to a release version (or image.digest) so an upgrade or a pod restart cannot change the collector by itself.
{{- end }}
{{- end -}}
