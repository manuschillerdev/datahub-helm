"""Exercise managed Kafka ingestion and a custom Actions pipeline inside Kubernetes."""

from collections.abc import Callable
from io import BytesIO
import json
import os
from pathlib import Path
import re
import struct
import time
from typing import TypeVar
from uuid import uuid4

from confluent_kafka import KafkaError, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from fastavro import schemaless_writer
import httpx
import requests
import yaml

from datahub.configuration.kafka import KafkaProducerConnectionConfig
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.schema_classes import DatasetProfileClass, SchemaMetadataClass

ACTION_NAME = "tls_lab_schema_observer"
SOURCE_URN = "urn:li:dataHubIngestionSource:tls-lab-kafka"
T = TypeVar("T")


def wait_for(description: str, check: Callable[[], T | None], timeout: int = 180) -> T:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result is not None:
            return result
        time.sleep(3)
    raise RuntimeError(f"Timed out waiting for {description}")


def seed_topic(connection: KafkaProducerConnectionConfig) -> str:
    config: dict[str, str | int | float | bool] = {
        "bootstrap.servers": connection.bootstrap
    }
    for key, value in connection.get_producer_config().items():
        if not isinstance(value, (str, int, float, bool)):
            raise ValueError(f"The lab requires a scalar Kafka property: {key}")
        config[key] = value
    admin = AdminClient(config)

    def action_ready() -> bool | None:
        try:
            group = admin.describe_consumer_groups([ACTION_NAME])[ACTION_NAME].result(
                20
            )
            return True if group.members else None
        except KafkaException:
            return None

    wait_for("custom Actions consumer group", action_ready)
    topic = "tls-lab.events." + uuid4().hex[:12]
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[
        topic
    ].result(30)
    schema = {
        "type": "record",
        "name": "LabEvent",
        "fields": [
            {"name": "event_id", "type": "long"},
            {"name": "message", "type": "string"},
        ],
    }
    registry = SchemaRegistryClient(connection.get_schema_registry_config())
    schema_id = registry.register_schema(
        topic + "-value", Schema(json.dumps(schema), "AVRO")
    )
    producer = Producer(config)
    errors: list[str] = []

    def delivered(error: KafkaError | None, message: Message) -> None:
        if error is not None:
            errors.append(str(error))

    for event_id in range(1, 4):
        payload = BytesIO(struct.pack(">bI", 0, schema_id))
        payload.seek(0, 2)
        schemaless_writer(
            payload, schema, {"event_id": event_id, "message": f"event-{event_id}"}
        )
        producer.produce(topic, value=payload.getvalue(), on_delivery=delivered)
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(
            f"Kafka delivery failed: {remaining} pending, errors={errors}"
        )
    print(
        f"Seeded {topic}: 3 Avro records; Schema Registry schema ID {schema_id}",
        flush=True,
    )
    return topic


def ingest(graph: DataHubGraph, topic: str) -> str:
    # Keep environment placeholders intact: credentials stay in pod Secrets and
    # the native executor uses the locally built package, without downloading a CLI.
    recipe = yaml.safe_load(Path("/etc/recipe/kafka.yaml").read_text())
    recipe["source"]["config"]["topic_patterns"]["allow"] = [
        "^" + re.escape(topic) + "$"
    ]
    graph.execute_graphql(
        """mutation($urn: String!, $input: UpdateIngestionSourceInput!) {
          updateIngestionSource(urn: $urn, input: $input)
        }""",
        variables={
            "urn": SOURCE_URN,
            "input": {
                "name": "TLS lab Kafka",
                "type": "kafka",
                "description": "Kafka and Schema Registry TLS with message profiling, executed in the Actions pod.",
                "config": {
                    "recipe": json.dumps(recipe),
                    "version": "native",
                    "executorId": "default",
                },
            },
        },
    )
    result = graph.execute_graphql(
        """mutation($input: CreateIngestionExecutionRequestInput!) {
          createIngestionExecutionRequest(input: $input)
        }""",
        variables={"input": {"ingestionSourceUrn": SOURCE_URN}},
    )
    execution_urn: str = result["createIngestionExecutionRequest"]
    print(f"Triggered {SOURCE_URN}: {execution_urn}", flush=True)
    previous_status = ""

    def completed() -> str | None:
        nonlocal previous_status
        response = graph.execute_graphql(
            """query($urn: String!) {
              executionRequest(urn: $urn) { result { status } }
            }""",
            variables={"urn": execution_urn},
        )
        result = (response.get("executionRequest") or {}).get("result") or {}
        status = result.get("status", "PENDING")
        if status != previous_status:
            print(f"Ingestion status: {status}", flush=True)
            previous_status = status
        if status == "SUCCESS":
            return execution_urn
        if status not in {"PENDING", "RUNNING"}:
            raise RuntimeError(
                f"Ingestion {execution_urn} ended with {status}; inspect its report in DataHub"
            )
        return None

    return wait_for("managed Kafka ingestion", completed, timeout=600)


def verify_metadata(graph: DataHubGraph, urn: str) -> DatasetProfileClass:
    expected = {"event_id", "message"}

    def schema_ready() -> SchemaMetadataClass | None:
        schema = graph.get_aspect(urn, SchemaMetadataClass)
        if (
            schema
            and {field.fieldPath.rsplit(".", 1)[-1] for field in schema.fields}
            == expected
        ):
            return schema
        return None

    wait_for("ingested Avro schema", schema_ready)

    def profile_ready() -> DatasetProfileClass | None:
        profile = graph.get_latest_timeseries_value(urn, DatasetProfileClass, {})
        if profile and profile.fieldProfiles:
            fields = {
                field.fieldPath.rsplit(".", 1)[-1]: field
                for field in profile.fieldProfiles
            }
            if set(fields) == expected and all(
                field.uniqueCount == 3 for field in fields.values()
            ):
                return profile
        return None

    profile = wait_for("profile of all three Kafka records", profile_ready)
    print(
        "Verified persisted schema and profiles: event_id and message each have 3 distinct values",
        flush=True,
    )
    return profile


def verify_action(urn: str) -> str:
    account = Path("/var/run/secrets/kubernetes.io/serviceaccount")
    namespace = (account / "namespace").read_text().strip()
    url = f"https://kubernetes.default.svc/api/v1/namespaces/{namespace}/pods"
    session = requests.Session()
    # Kubernetes uses its own CA, distinct from the lab's Kafka/Keycloak CA.
    session.trust_env = False
    session.verify = str(account / "ca.crt")
    session.headers["Authorization"] = (
        "Bearer " + (account / "token").read_text().strip()
    )
    decoder = json.JSONDecoder()

    def observed() -> str | None:
        response = session.get(
            url,
            params={"labelSelector": "app.kubernetes.io/name=acryl-datahub-actions"},
            timeout=20,
        )
        response.raise_for_status()
        for pod in response.json()["items"]:
            if pod["status"]["phase"] != "Running":
                continue
            name = pod["metadata"]["name"]
            response = session.get(
                f"{url}/{name}/log",
                params={"container": "acryl-datahub-actions", "tailLines": 10000},
                timeout=20,
            )
            response.raise_for_status()
            for chunk in response.text.split("Hello world! Received event:")[1:]:
                try:
                    envelope, _ = decoder.raw_decode(chunk.lstrip())
                except json.JSONDecodeError:
                    continue
                event = envelope.get("event", {})
                if (
                    envelope.get("event_type") == "MetadataChangeLogEvent_v1"
                    and event.get("entityUrn") == urn
                    and event.get("aspectName") == "schemaMetadata"
                ):
                    return name
        return None

    return wait_for("custom Actions output for the fresh dataset", observed)


def main() -> None:
    public_url = "https://pypi.org/simple/acryl-datahub/"
    requests.get(public_url, timeout=30).raise_for_status()
    httpx.get(public_url, timeout=30).raise_for_status()
    print("Verified public HTTPS trust with Requests and HTTPX", flush=True)
    connection = KafkaProducerConnectionConfig(
        bootstrap=os.environ["KAFKA_BOOTSTRAP_SERVER"],
        schema_registry_url=os.environ["SCHEMA_REGISTRY_URL"],
    )
    topic = seed_topic(connection)
    urn = f"urn:li:dataset:(urn:li:dataPlatform:kafka,{topic},PROD)"
    graph = DataHubGraph(DatahubClientConfig(server="http://datahub-datahub-gms:8080"))
    execution_urn = ingest(graph, topic)
    profile = verify_metadata(graph, urn)
    action_pod = verify_action(urn)
    print(
        json.dumps(
            {
                "status": "PASS",
                "kafka_security_protocol": connection.get_producer_config()[
                    "security.protocol"
                ],
                "source_urn": SOURCE_URN,
                "execution_urn": execution_urn,
                "dataset_urn": urn,
                "profile_partition": profile.partitionSpec.partition
                if profile.partitionSpec
                else None,
                "actions_pipeline": ACTION_NAME,
                "actions_pod": action_pod,
            },
            indent=2,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
