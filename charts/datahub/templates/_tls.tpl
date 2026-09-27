{{/* PEM TLS is opt-in. Secret references are validated even for CA-only TLS. */}}
{{- define "datahub.tls.validate" -}}
{{- $tls := .Values.global.tls | default dict -}}
{{- if $tls.enabled -}}
{{- if and $tls.keyPasswordFile (not $tls.client) -}}
{{- fail "global.tls.keyPasswordFile requires a client PEM bundle" -}}
{{- end -}}
{{- range $name := list "ca" "client" "keyPasswordFile" -}}
{{- with index $tls $name -}}
{{- $_ := required (printf "global.tls.%s.secretName is required" $name) .secretName -}}
{{- $_ := required (printf "global.tls.%s.key is required" $name) .key -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "datahub.tls.volumes" -}}
{{- $tls := .Values.global.tls | default dict -}}
{{- if and $tls.enabled (or $tls.ca $tls.client) }}
- name: datahub-tls
  projected:
    defaultMode: 0440
    sources:
      {{- range $name, $path := dict "ca" "ca.pem" "client" "client.pem" }}
      {{- with index $tls $name }}
      - secret:
          name: {{ .secretName | quote }}
          items:
            - key: {{ .key | quote }}
              path: {{ $path | quote }}
      {{- end }}
      {{- end }}
{{- end }}
{{- end -}}

{{- define "datahub.tls.mounts" -}}
{{- $tls := .Values.global.tls | default dict -}}
{{- if and $tls.enabled (or $tls.ca $tls.client) }}
- name: datahub-tls
  mountPath: /mnt/datahub/tls
  readOnly: true
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
{{- if $tls.client -}}
{{- $_ := set $props "ssl.keystore.type" "PEM" -}}
{{- $_ := set $props "ssl.keystore.location" "/mnt/datahub/tls/client.pem" -}}
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
{{- if $tls.ca -}}
{{- $_ := set $broker "ssl.ca.location" "/mnt/datahub/tls/ca.pem" -}}
{{- $_ := set $registry "ssl.ca.location" "/mnt/datahub/tls/ca.pem" -}}
{{- end -}}
{{- if $tls.client -}}
{{- range $property := list "ssl.certificate.location" "ssl.key.location" -}}
{{- $_ := set $broker $property "/mnt/datahub/tls/client.pem" -}}
{{- $_ := set $registry $property "/mnt/datahub/tls/client.pem" -}}
{{- end -}}
{{- end -}}
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
