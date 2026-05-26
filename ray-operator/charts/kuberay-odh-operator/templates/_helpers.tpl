{{/*
Expand the name of the chart.
*/}}
{{- define "kuberay-odh-operator.name" -}}
kuberay-operator
{{- end }}

{{/*
Common labels applied to all resources.
*/}}
{{- define "kuberay-odh-operator.labels" -}}
app.kubernetes.io/name: {{ include "kuberay-odh-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: kuberay-operator
{{- end }}

{{/*
Selector labels for the operator Deployment.
*/}}
{{- define "kuberay-odh-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kuberay-odh-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Build the --feature-gates flag value from the featureGates list.
*/}}
{{- define "kuberay-odh-operator.featureGates" -}}
{{- $gates := list -}}
{{- range .Values.featureGates -}}
{{- $gates = append $gates (printf "%s=%t" .name .enabled) -}}
{{- end -}}
{{- if $gates -}}
--feature-gates={{ join "," $gates }}
{{- end -}}
{{- end }}
