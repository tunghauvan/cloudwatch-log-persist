{{/*
Expand the name of the chart.
*/}}
{{- define "cloudwatch-log-persist.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "cloudwatch-log-persist.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "cloudwatch-log-persist.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "cloudwatch-log-persist.labels" -}}
helm.sh/chart: {{ include "cloudwatch-log-persist.chart" . }}
{{ include "cloudwatch-log-persist.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "cloudwatch-log-persist.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cloudwatch-log-persist.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "cloudwatch-log-persist.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "cloudwatch-log-persist.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "cloudwatch-log-persist.postgresqlFullname" -}}
{{- printf "%s-postgresql" (include "cloudwatch-log-persist.fullname" .) | trunc 58 | trimSuffix "-" }}
{{- end }}
