# Local TLS and OAuth lab

This lab exercises the PEM TLS changes in matching DataHub backend and Helm
worktrees. It runs on the **default Colima Docker profile**, in namespace
`datahub-tls`, with an isolated kubeconfig. It does not change your default
Kubernetes context. The launcher refuses non-loopback API endpoints and nodes
other than `colima`.

The dependencies are Redpanda (Kafka and Schema Registry), Keycloak (DataHub SSO
and Kafka OAuth), cert-manager (a private CA and client/server certificates),
MySQL and OpenSearch. This is a local development environment: SQL and search
use the cluster network without TLS, Keycloak uses its development database,
and Kafka authorization is disabled so this lab isolates authentication from
ACL policy.

Redpanda OAuth requires an Enterprise license. A fresh Redpanda cluster includes
[a 30-day evaluation](https://docs.redpanda.com/current/get-started/licensing/).
Use a valid license for continued OAuth testing after that evaluation.

## Start

Use the backend worktree's environment wrapper for every lifecycle command. The
backend's `mise.toml` pins the required tools. Colima must have the Docker runtime
and enough resources for the stack and image builds; the reference machine uses
4 CPUs and 32 GiB for its Colima VM. Building all four images also needs substantial
free host disk space (allow at least 25 GiB).

```sh
cd /path/to/datahub.fix-pem-tls
mise install
scripts/dev/datahub-dev.sh tls cluster
scripts/dev/datahub-dev.sh tls dependencies --helm-dir ../datahub-helm.fix-pem-tls
scripts/dev/datahub-dev.sh tls build
scripts/dev/datahub-dev.sh tls deploy --helm-dir ../datahub-helm.fix-pem-tls --mode oauth
scripts/dev/datahub-dev.sh tls status
scripts/dev/datahub-dev.sh tls verify
scripts/dev/datahub-dev.sh tls forward
```

`cluster` enables Kubernetes in the existing default Colima profile. `build`
compiles and builds GMS, system-update, Actions (including the local Python
package and Kafka source dependencies) and frontend from this worktree, directly
in Colima's Docker daemon. To rebuild just Actions, use
`tls build --component datahub-actions`.
Images use a source-derived local tag and `imagePullPolicy: Never`; published
images predating these fixes will not exercise the same behavior.

Keep `forward` running while using the browser:

- DataHub: <http://localhost:9002>
- Keycloak: <https://localhost:9443>
- GMS: <http://localhost:18080>

Retrieve the generated login credentials in a separate terminal:

```sh
scripts/dev/datahub-dev.sh tls login
```

The DataHub SSO user is `tls-user`; the Keycloak administrator is `admin`.
Credentials and the isolated kubeconfig live under the ignored
`build/tls-lab/` directory with restrictive permissions. Do not commit that
directory. Preserve it when cleaning build artifacts: these passwords match
the retained database and Keycloak volumes. The public root certificate is exported to `build/tls-lab/ca.crt`.
Import that CA into your browser's trust store if needed, then use the SSO login
on DataHub. The launcher does not modify the system keychain.

Keycloak advertises `https://localhost:9443` for browser authorization and its
issuer, with dynamic HTTPS backchannel endpoints under `https://keycloak` for
in-cluster token, discovery and JWKS requests. Both names are present in its
certificate. DataHub's frontend imports the public CA into a temporary Java
truststore on startup. Broker and Schema Registry clients use PEM directly.

## Authentication profiles

The default `--mode oauth` requires a client certificate **and** a Keycloak
OAuth bearer token on Kafka port 9094. Java receives a JAAS configuration through
a Kubernetes Secret; Python uses librdkafka's native OIDC client. Schema Registry
requires a client certificate on HTTPS port 8081 in both profiles.

To isolate certificate authentication from OAuth:

```sh
scripts/dev/datahub-dev.sh tls deploy --helm-dir ../datahub-helm.fix-pem-tls --mode mtls
```

This selects Kafka port 9093 with TLS client authentication. Restore the full
profile with `--mode oauth`. These commands use `--reset-values`, so Java JAAS
and Python OAuth overrides do not linger when switching profiles. DataHub SSO
continues to use Keycloak in both profiles.

## Certificates and iteration

cert-manager creates the `lab-ca`, `keycloak-server`, `datahub-client` and
Redpanda certificates. The example requests `CombinedPEM` for the client
certificate and supplies the resulting `tls-combined.pem` Secret entry through
`global.tls.client`. Its RSA PKCS#8 private key and certificate chain are mounted
directly for both Java and Python. This is the example's certificate delivery
choice; the DataHub chart accepts an operator-provided PEM bundle from any
source and has no dependency on cert-manager.

Certificates renew automatically, but this does not guarantee that running
clients reload them. Restart the affected workloads after renewal. The client
PEM bundle is mounted directly from the Secret; the frontend truststore is
regenerated by its example-specific init container at pod startup. This lab does not install an automatic workload reloader. Replacing
the root CA requires reissuing dependent certificates and updating browser
trust as well.

After backend changes, run `tls build` and `tls deploy` again. After dependency
chart changes, run `tls dependencies`; existing volumes and generated passwords
are retained. Keycloak imports a realm only on first creation, so editing
`realm.json` does not overwrite an existing realm. Use the Keycloak admin UI for
changes to a running lab.

`Ctrl-C` stops port forwarding; workloads stay running for continued testing.
The namespace, PVCs and cert-manager installation are deliberately retained.

## In-cluster Actions and Kafka source verification

```sh
scripts/dev/datahub-dev.sh tls verify
```

All application and verification processes run in Kubernetes containers. The
launcher only creates a Job from the Helm-managed, suspended `tls-lab-verify`
CronJob and streams its logs using the isolated Colima kubeconfig. Each Job has a
15-minute deadline, no retries, and is retained for seven days for inspection.

The dependency chart installs the Actions config and Kafka recipe as ConfigMaps.
The main DataHub chart mounts `tls-lab-actions` into the Actions pod and supplies
its normal Kafka TLS/OAuth environment. Its custom `tls_lab_schema_observer`
pipeline uses the built-in `hello_world` action, filtered to dataset schema
changes. A successful run proves it consumed and deserialized a fresh metadata
event; pod readiness or consumer-group membership alone does not count.

The verification Job:

1. Checks public HTTPS trust with Requests and HTTPX, then creates a unique Kafka
   topic and publishes three Avro records through the selected TLS/OAuth listener.
   It registers the schema over Schema Registry HTTPS with a client certificate.
2. Creates or updates **TLS lab Kafka** in DataHub's ingestion UI and requests a
   run through GraphQL. The recipe uses `version: native`, so the ingestion
   executor runs the locally built CLI **inside the Actions pod**. The lab image
   includes the full `kafka` extra, including gRPC/Protobuf schema dependencies.
3. Requires a successful execution and reads back the dataset's two schema fields
   and profiles. Each field must contain three distinct sampled values, proving
   the Kafka source consumed messages as well as listing broker metadata.
4. Reads Actions pod logs through the Kubernetes API and matches the fresh
   dataset's `schemaMetadata` event. Its ServiceAccount can only read pods and pod
   logs in the lab namespace.

The Job prints a `PASS` summary with the dataset, execution and source URNs.
The registered source remains available for reruns in the UI; it targets the
latest generated topic. Previous fixture topics and datasets are retained, and
stateful deletion is disabled. Run verification serially because it updates one
shared ingestion-source definition. No credentials are stored in the recipe.

When updating these fixtures, run `tls dependencies` followed by `tls deploy`
before `tls verify`. The source's Kafka and Schema Registry settings come from
the same Helm TLS helpers as the Actions pipeline and verification Job.
Switching between `--mode oauth` and `--mode mtls` changes all three workloads.

Python HTTP clients use their image's default public trust roots. The example
does not set `REQUESTS_CA_BUNDLE` or `SSL_CERT_FILE` on DataHub workloads and has
no CA-merging init container. GMS uses HTTP inside this example; Kafka, Schema
Registry and the Kafka OAuth token client use their dedicated private-CA
settings. For an HTTPS GMS with a private CA, supply a complete HTTP trust bundle
through the workload's volume and environment settings as described in the
[chart documentation](../../charts/datahub/README.md#outbound-pem-tls).

## Verified locally

On 2026-09-26, the Colima deployment completed with all service pods ready and
both system-update jobs successful. Manual checks confirmed:

- Keycloak SSO completed and DataHub returned `urn:li:corpuser:tls-user`.
- The local Python Kafka emitter registered/read Avro schemas over HTTPS mTLS,
  published a dataset through Kafka OAuth with a client certificate, and the
  Java consumer persisted its properties in GMS.
- The independent Kafka mTLS listener returned broker/topic metadata.
- Both bundled Actions consumer groups joined successfully.
- The Helm-managed verification Job passed in OAuth mode: the registered Kafka
  source ran in Actions, extracted both Avro fields, and profiled three messages.
  The custom `tls_lab_schema_observer` action logged that fresh schema event.
- Requests, HTTPX and a non-mutating `uv pip install --dry-run` package lookup
  succeeded against PyPI.
- Schema Registry rejected a request that omitted the client certificate.

On 2026-09-27, `tls verify` passed after removing automatic Python CA merging
and HTTP trust environment variables from the chart. The new Actions and
verification pods used image defaults for public HTTPS, while Kafka OAuth/mTLS,
Schema Registry mTLS, source profiling and Actions event consumption passed.
A fresh `uv pip install --dry-run --no-deps --refresh` lookup inside Actions
also succeeded without changing installed packages.

The sample dataset is named **TLS lab example**, with URN
`urn:li:dataset:(urn:li:dataPlatform:postgres,tls_lab.example_table,PROD)`.
Encrypted client keys and certificate renewal/rollout have not been exercised
in this live deployment. The on-demand verification above is a Kubernetes lab
scenario; no unit or regression test suites were added.
