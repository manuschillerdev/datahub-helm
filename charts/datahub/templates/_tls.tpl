{{/* PEM TLS is opt-in. Secret references are validated even for CA-only TLS. */}}
{{- define "datahub.tls.validate" -}}
{{- $tls := .Values.global.tls | default dict -}}
{{- if $tls.enabled -}}
{{- if ne (not (empty $tls.cert)) (not (empty $tls.key)) -}}
{{- fail "global.tls.cert and global.tls.key must be configured together" -}}
{{- end -}}
{{- if and $tls.keyPasswordFile (not $tls.key) -}}
{{- fail "global.tls.keyPasswordFile requires a client certificate and key" -}}
{{- end -}}
{{- range $name := list "ca" "cert" "key" "keyPasswordFile" -}}
{{- with index $tls $name -}}
{{- $_ := required (printf "global.tls.%s.secretName is required" $name) .secretName -}}
{{- $_ := required (printf "global.tls.%s.key is required" $name) .key -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "datahub.tls.volumes" -}}
{{- $tls := .context.Values.global.tls | default dict -}}
{{- if and $tls.enabled (or $tls.ca $tls.cert) }}
- name: datahub-tls
  projected:
    defaultMode: 0440
    sources:
      {{- range $name, $path := dict "ca" "ca.pem" "cert" "tls.crt" "key" "tls.key" }}
      {{- with index $tls $name }}
      - secret:
          name: {{ .secretName | quote }}
          items:
            - key: {{ .key | quote }}
              path: {{ $path | quote }}
      {{- end }}
      {{- end }}
{{- if and .java $tls.cert }}
- name: datahub-tls-keystore
  emptyDir:
    medium: Memory
{{- end }}
{{- end }}
{{- end -}}

{{- define "datahub.tls.mounts" -}}
{{- $tls := .context.Values.global.tls | default dict -}}
{{- if and $tls.enabled (or $tls.ca $tls.cert) }}
- name: datahub-tls
  mountPath: /mnt/datahub/tls
  readOnly: true
{{- if and .java $tls.cert }}
- name: datahub-tls-keystore
  mountPath: /mnt/datahub/tls-keystore
  readOnly: true
{{- end }}
{{- end }}
{{- end -}}

{{/* Reuse the workload image and UID so the private keystore stays mode 0600. */}}
{{- define "datahub.tls.initContainer" -}}
{{- $tls := .context.Values.global.tls | default dict -}}
{{- if and $tls.enabled $tls.cert }}
- name: prepare-tls-keystore
  image: {{ include "datahub.image" (dict "imageRegistry" .context.Values.global.imageRegistry "version" .context.Values.global.datahub.version "image" .image) }}
  imagePullPolicy: {{ .image.pullPolicy | default "IfNotPresent" }}
  securityContext:
    {{- toYaml .securityContext | nindent 4 }}
  command:
    - /bin/sh
    - -ec
    - |
      umask 077
      { cat /mnt/datahub/tls/tls.crt; printf '\n'; cat /mnt/datahub/tls/tls.key; } > /mnt/datahub/tls-keystore/client.pem
  volumeMounts:
    - name: datahub-tls
      mountPath: /mnt/datahub/tls
      readOnly: true
    - name: datahub-tls-keystore
      mountPath: /mnt/datahub/tls-keystore
{{- end }}
{{- end -}}

{{/* Explicit Spring properties and secret-backed settings win over TLS defaults. */}}
{{- define "datahub.tls.javaEnv" -}}
{{- $root := .context -}}
{{- $tls := $root.Values.global.tls | default dict -}}
{{- if $tls.enabled -}}
{{- $overrides := $root.Values.global.springKafkaConfigurationOverrides | default dict -}}
{{- $credentials := $root.Values.global.credentialsAndCertsSecrets | default dict -}}
{{- $secrets := $credentials.secureEnv | default dict -}}
{{- $props := dict -}}
{{- if not $root.Values.global.kafka.iam.enabled -}}
{{- $_ := set $props "security.protocol" "SSL" -}}
{{- else if not .iamConfigured -}}
{{- $_ := set $props "security.protocol" "SASL_SSL" -}}
{{- $_ := set $props "sasl.mechanism" "AWS_MSK_IAM" -}}
{{- $_ := set $props "sasl.jaas.config" "software.amazon.msk.auth.iam.IAMLoginModule required;" -}}
{{- $_ := set $props "sasl.client.callback.handler.class" "software.amazon.msk.auth.iam.IAMClientCallbackHandler" -}}
{{- end -}}
{{- if $tls.ca -}}
{{- $_ := set $props "ssl.truststore.type" "PEM" -}}
{{- $_ := set $props "ssl.truststore.location" "/mnt/datahub/tls/ca.pem" -}}
{{- end -}}
{{- if $tls.cert -}}
{{- $_ := set $props "ssl.keystore.type" "PEM" -}}
{{- $_ := set $props "ssl.keystore.location" "/mnt/datahub/tls-keystore/client.pem" -}}
{{- end -}}
{{- range $key, $value := $props }}
{{- if not (or (hasKey $overrides $key) (hasKey $secrets $key)) }}
- name: {{ $.prefix }}{{ $key | replace "." "_" | upper }}
  value: {{ $value | quote }}
{{- end }}
{{- end }}
{{- if and $tls.keyPasswordFile (not (or (hasKey $overrides "ssl.key.password") (hasKey $secrets "ssl.key.password"))) }}
- name: {{ .prefix }}SSL_KEY_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $tls.keyPasswordFile.secretName | quote }}
      key: {{ $tls.keyPasswordFile.key | quote }}
{{- end }}
{{- if eq $root.Values.global.kafka.schemaregistry.type "KAFKA" }}
{{- range $key, $value := $props }}
{{- if hasPrefix "ssl." $key }}
{{- $schemaKey := printf "schema.registry.%s" $key }}
- name: KAFKA_SCHEMA_REGISTRY_{{ $key | replace "." "_" | upper }}
  {{- if hasKey $secrets $schemaKey }}
  valueFrom:
    secretKeyRef:
      name: {{ $credentials.name | quote }}
      key: {{ index $secrets $schemaKey | quote }}
  {{- else }}
  value: {{ (get $overrides $schemaKey | default $value) | quote }}
  {{- end }}
{{- if not (or (hasKey $overrides $schemaKey) (hasKey $secrets $schemaKey)) }}
- name: {{ $.prefix }}SCHEMA_REGISTRY_{{ $key | replace "." "_" | upper }}
  value: {{ $value | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- if $tls.keyPasswordFile }}
- name: KAFKA_SCHEMA_REGISTRY_SSL_KEY_PASSWORD
  {{- if hasKey $secrets "schema.registry.ssl.key.password" }}
  valueFrom:
    secretKeyRef:
      name: {{ $credentials.name | quote }}
      key: {{ index $secrets "schema.registry.ssl.key.password" | quote }}
  {{- else if hasKey $overrides "schema.registry.ssl.key.password" }}
  value: {{ index $overrides "schema.registry.ssl.key.password" | quote }}
  {{- else }}
  valueFrom:
    secretKeyRef:
      name: {{ $tls.keyPasswordFile.secretName | quote }}
      key: {{ $tls.keyPasswordFile.key | quote }}
  {{- end }}
{{- if not (or (hasKey $overrides "schema.registry.ssl.key.password") (hasKey $secrets "schema.registry.ssl.key.password")) }}
- name: {{ .prefix }}SCHEMA_REGISTRY_SSL_KEY_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $tls.keyPasswordFile.secretName | quote }}
      key: {{ $tls.keyPasswordFile.key | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{- define "datahub.tls.pythonEnv" -}}
{{- $tls := .Values.global.tls | default dict -}}
{{- $broker := dict -}}
{{- $registry := dict -}}
{{- if $tls.enabled -}}
{{- if not .Values.global.kafka.iam.enabled -}}
{{- $_ := set $broker "security.protocol" "SSL" -}}
{{- end -}}
{{- range $name, $property := dict "ca" "ssl.ca.location" "cert" "ssl.certificate.location" "key" "ssl.key.location" -}}
{{- if index $tls $name -}}
{{- $path := index (dict "ca" "/mnt/datahub/tls/ca.pem" "cert" "/mnt/datahub/tls/tls.crt" "key" "/mnt/datahub/tls/tls.key") $name -}}
{{- $_ := set $broker $property $path -}}
{{- $_ := set $registry $property $path -}}
{{- end -}}
{{- end -}}
{{- if $tls.ca }}
- name: REQUESTS_CA_BUNDLE
  value: /mnt/datahub/tls/ca.pem
- name: SSL_CERT_FILE
  value: /mnt/datahub/tls/ca.pem
{{- end }}
{{- end -}}
{{- $overrides := .Values.global.pythonKafkaConfigurationOverrides | default dict -}}
{{- if not $overrides -}}
{{- range $key, $value := .Values.global.springKafkaConfigurationOverrides -}}
{{- if and (ne (include "datahub.kafka.is-java-only" (dict "propName" $key)) "true") (not (hasPrefix "schema.registry." $key)) (not (and (eq $key "partition.assignment.strategy") (contains "org.apache.kafka." (toString $value)))) -}}
{{- $_ := set $overrides $key $value -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $broker = mergeOverwrite $broker $overrides -}}
{{- $registry = mergeOverwrite $registry (.Values.global.pythonKafkaSchemaRegistryConfigurationOverrides | default dict) -}}
{{- $secrets := .Values.global.pythonKafkaSecretsOverrides | default dict -}}
{{- if not $secrets -}}
{{- with .Values.global.credentialsAndCertsSecrets -}}
{{- $secretName := .name -}}
{{- range $key, $value := .secureEnv -}}
{{- if and (ne (include "datahub.kafka.is-java-only" (dict "propName" $key)) "true") (not (hasPrefix "schema.registry." $key)) -}}
{{- $_ := set $secrets $key (dict "secretRef" $secretName "secretKey" $value) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- range $prefix, $props := dict "KAFKA_PROPERTIES_" $broker "KAFKA_SCHEMA_REGISTRY_PROPERTIES_" $registry }}
{{- range $key, $value := $props }}
{{- if not (and (eq $prefix "KAFKA_PROPERTIES_") (or (hasKey $secrets $key) (and $.Values.global.kafka.iam.enabled (eq $key "security.protocol")))) }}
- name: {{ $prefix }}{{ $key | replace "." "_" | upper }}
  value: {{ $value | quote }}
{{- end }}
{{- end }}
{{- if and $tls.enabled $tls.keyPasswordFile (not (hasKey $props "ssl.key.password")) (not (and (eq $prefix "KAFKA_PROPERTIES_") (hasKey $secrets "ssl.key.password"))) }}
- name: {{ $prefix }}SSL_KEY_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ $tls.keyPasswordFile.secretName | quote }}
      key: {{ $tls.keyPasswordFile.key | quote }}
{{- end }}
{{- end }}
{{- range $key, $secret := $secrets }}
{{- if not (and $.Values.global.kafka.iam.enabled (eq $key "security.protocol")) }}
- name: KAFKA_PROPERTIES_{{ $key | replace "." "_" | upper }}
  valueFrom:
    secretKeyRef:
      name: {{ $secret.secretRef | quote }}
      key: {{ $secret.secretKey | quote }}
{{- end }}
{{- end }}
{{- end -}}
