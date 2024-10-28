#!/bin/bash
set -e

# 1. Generating a x509 (CA) cert from a private key:
openssl genrsa -out certs/ca.key 4096
openssl req -new -x509 -key certs/ca.key -days 87660 -subj "/CN=kafka-ca" -out certs/ca.crt

# 2. Generating a private key for kafka server and csr:
openssl genrsa -out certs/kafka.key 4096
openssl req -new -nodes -key certs/kafka.key -out certs/kafka.csr -subj "/CN=kafka"
openssl x509 -req -in certs/kafka.csr -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial -out certs/kafka.crt -days 3650 -extensions SAN -extfile <(printf "[SAN]\nsubjectAltName=DNS:localhost")

# 3. Generating keystore for kafka server:
openssl pkcs12 -export -in certs/kafka.crt \
    -passout pass:password \
    -inkey certs/kafka.key \
    -out certs/kafka.keystore.jks

# 4. Generating truststore for kafka server:
keytool -importkeystore -srckeystore certs/kafka.keystore.jks \
    -srcstoretype PKCS12 \
    -srcstorepass password \
    -deststorepass password \
    -destkeystore certs/kafka.truststore.jks \
    -noprompt

rm certs/{ca.key,ca.srl,kafka.crt,kafka.csr,kafka.key}
