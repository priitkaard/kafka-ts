#!/bin/bash
set -e

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

KAFKA_JVM_PERFORMANCE_OPTS="-Djava.security.manager=allow" kafka-configs --bootstrap-server localhost:9092 --command-config "$SCRIPT_DIR/kafka-local.properties" --alter --add-config 'SCRAM-SHA-256=[password=admin]' --entity-type users --entity-name admin
KAFKA_JVM_PERFORMANCE_OPTS="-Djava.security.manager=allow" kafka-configs --bootstrap-server localhost:9092 --command-config "$SCRIPT_DIR/kafka-local.properties" --alter --add-config 'SCRAM-SHA-512=[password=admin]' --entity-type users --entity-name admin
