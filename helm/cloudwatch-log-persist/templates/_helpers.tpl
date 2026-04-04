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
Postgres secret name — use existingSecret if set, otherwise the auto-created one.
*/}}
{{- define "cloudwatch-log-persist.postgresSecretName" -}}
{{- if .Values.postgres.auth.existingSecret }}
{{- .Values.postgres.auth.existingSecret }}
{{- else }}
{{- printf "%s-postgres-secret" (include "cloudwatch-log-persist.fullname" .) }}
{{- end }}
{{- end }}

{{/*
MinIO secret name — use existingSecret if set, otherwise the auto-created one.
*/}}
{{- define "cloudwatch-log-persist.minioSecretName" -}}
{{- if .Values.minio.auth.existingSecret }}
{{- .Values.minio.auth.existingSecret }}
{{- else }}
{{- printf "%s-minio-secret" (include "cloudwatch-log-persist.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Resolved PostgreSQL host — use external if disabled, otherwise in-chart service.
*/}}
{{- define "cloudwatch-log-persist.postgresHost" -}}
{{- if not .Values.postgres.enabled }}
{{- .Values.postgres.external.host }}
{{- else }}
{{- printf "%s-postgres" (include "cloudwatch-log-persist.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Resolved PostgreSQL port.
*/}}
{{- define "cloudwatch-log-persist.postgresPort" -}}
{{- if not .Values.postgres.enabled }}
{{- default 5432 .Values.postgres.external.port }}
{{- else }}
{{- 5432 }}
{{- end }}
{{- end }}

{{/*
Resolved PostgreSQL database name.
*/}}
{{- define "cloudwatch-log-persist.postgresDatabase" -}}
{{- if not .Values.postgres.enabled }}
{{- .Values.postgres.external.database }}
{{- else }}
{{- .Values.postgres.auth.database }}
{{- end }}
{{- end }}

{{/*
Resolved PostgreSQL username.
*/}}
{{- define "cloudwatch-log-persist.postgresUsername" -}}
{{- if not .Values.postgres.enabled }}
{{- .Values.postgres.external.username }}
{{- else }}
{{- .Values.postgres.auth.username }}
{{- end }}
{{- end }}

{{/*
Resolved MinIO/S3 endpoint — use external if disabled, otherwise in-chart service.
*/}}
{{- define "cloudwatch-log-persist.minioEndpoint" -}}
{{- if not .Values.minio.enabled }}
{{- .Values.minio.external.endpoint }}
{{- else }}
{{- printf "http://%s-minio:%d" (include "cloudwatch-log-persist.fullname" .) (int .Values.minio.service.port) }}
{{- end }}
{{- end }}

{{/*
Resolved S3 bucket name.
*/}}
{{- define "cloudwatch-log-persist.s3BucketName" -}}
{{- if not .Values.minio.enabled }}
{{- .Values.minio.external.bucketName }}
{{- else }}
{{- .Values.minio.bucketName }}
{{- end }}
{{- end }}

{{/*
Resolved S3 region.
*/}}
{{- define "cloudwatch-log-persist.s3Region" -}}
{{- if not .Values.minio.enabled }}
{{- .Values.minio.external.region }}
{{- else }}
{{- default "us-east-1" .Values.cloudwatchService.config.s3.region }}
{{- end }}
{{- end }}

{{/*
S3 access key — for MinIO or external S3
*/}}
{{- define "cloudwatch-log-persist.s3AccessKey" -}}
{{- if not .Values.minio.enabled }}
{{- .Values.minio.external.accessKey }}
{{- else }}
{{- .Values.minio.auth.rootUser }}
{{- end }}
{{- end }}

{{/*
S3 secret key — for MinIO or external S3
*/}}
{{- define "cloudwatch-log-persist.s3SecretKey" -}}
{{- if not .Values.minio.enabled }}
{{- .Values.minio.external.secretKey }}
{{- else }}
{{- .Values.minio.auth.rootPassword }}
{{- end }}
{{- end }}
