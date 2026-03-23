{{- define "bobravoz.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "bobravoz.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "bobravoz.labels" -}}
app.kubernetes.io/name: {{ include "bobravoz.name" . }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
{{- end -}}

{{- define "bobravoz.selectorLabels" -}}
app.kubernetes.io/name: {{ include "bobravoz.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "bobravoz.namespace" -}}
{{- if .Values.managementNamespace }}
{{- .Values.managementNamespace -}}
{{- else -}}
{{- .Release.Namespace -}}
{{- end -}}
{{- end -}}

{{- define "bobravoz.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
  {{- if .Values.serviceAccount.name -}}
{{- .Values.serviceAccount.name -}}
  {{- else -}}
{{- printf "%s-controller-manager" (include "bobravoz.fullname" .) | trunc 63 | trimSuffix "-" -}}
  {{- end -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "bobravoz.webhookCertSecretName" -}}
{{- if .Values.webhook.certSecretName }}
{{- .Values.webhook.certSecretName -}}
{{- else -}}
{{- printf "%s-webhook-server-cert" (include "bobravoz.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "bobravoz.metricsCertSecretName" -}}
{{- if .Values.metrics.tls.secretName }}
{{- .Values.metrics.tls.secretName -}}
{{- else -}}
{{- printf "%s-metrics-cert" (include "bobravoz.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "bobravoz.effectiveConfigNamespace" -}}
{{- if .Values.manager.configNamespace }}
{{- .Values.manager.configNamespace -}}
{{- else -}}
{{- include "bobravoz.namespace" . -}}
{{- end -}}
{{- end -}}

{{- define "bobravoz.configName" -}}
{{- if .Values.manager.configName }}
{{- .Values.manager.configName -}}
{{- else -}}
{{- printf "%s-operator-config" (include "bobravoz.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "bobravoz.hubCertSecretName" -}}
{{- if .Values.tls.hubSecretName }}
{{- .Values.tls.hubSecretName -}}
{{- else -}}
{{- printf "%s-hub-tls" (include "bobravoz.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
