{{/*
Expand the name of the chart.
*/}}
{{- define "bobrapet.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "bobrapet.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "bobrapet.labels" -}}
app.kubernetes.io/name: {{ include "bobrapet.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "bobrapet.selectorLabels" -}}
app.kubernetes.io/name: {{ include "bobrapet.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Namespace helper
*/}}
{{- define "bobrapet.namespace" -}}
{{- if .Values.managementNamespace }}
{{- .Values.managementNamespace -}}
{{- else -}}
{{- .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{/*
Leader election namespace helper
*/}}
{{- define "bobrapet.leaderElectionNamespace" -}}
{{- if .Values.manager.leaderElectionNamespace -}}
{{- .Values.manager.leaderElectionNamespace -}}
{{- else -}}
{{- include "bobrapet.namespace" . -}}
{{- end -}}
{{- end -}}

{{/*
Namespace for operator config (allows overriding separately)
*/}}
{{- define "bobrapet.effectiveConfigNamespace" -}}
{{- if .Values.manager.configNamespace }}
{{- .Values.manager.configNamespace -}}
{{- else -}}
{{- include "bobrapet.namespace" . -}}
{{- end -}}
{{- end -}}

{{/*
Operator config name helper
*/}}
{{- define "bobrapet.configName" -}}
{{- if .Values.manager.configName }}
{{- .Values.manager.configName -}}
{{- else -}}
{{- printf "%s-operator-config" (include "bobrapet.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Webhook certificate secret name helper
*/}}
{{- define "bobrapet.webhookCertSecretName" -}}
{{- if .Values.webhook.certSecretName }}
{{- .Values.webhook.certSecretName -}}
{{- else -}}
{{- printf "%s-webhook-server-cert" (include "bobrapet.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Metrics certificate secret helper
*/}}
{{- define "bobrapet.metricsCertSecretName" -}}
{{- if .Values.metrics.tls.secretName }}
{{- .Values.metrics.tls.secretName -}}
{{- else -}}
{{- printf "%s-metrics-cert" (include "bobrapet.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
serviceAccountName helper
*/}}
{{- define "bobrapet.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
  {{- if .Values.serviceAccount.name -}}
{{- .Values.serviceAccount.name -}}
  {{- else -}}
{{- printf "%s-controller-manager" (include "bobrapet.fullname" .) | trunc 63 | trimSuffix "-" -}}
  {{- end -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
