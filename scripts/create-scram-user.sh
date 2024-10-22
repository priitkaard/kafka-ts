#!/bin/bash
set -e

kafka-configs --bootstrap-server localhost:9092 --command-config kafka-local.properties --alter --add-config 'SCRAM-SHA-256=[password=admin]' --entity-type users --entity-name admin
kafka-configs --bootstrap-server localhost:9092 --command-config kafka-local.properties --alter --add-config 'SCRAM-SHA-512=[password=admin]' --entity-type users --entity-name admin
