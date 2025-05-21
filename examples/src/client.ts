import { readFileSync } from 'fs';
import { createKafkaClient, saslScramSha512, setTracer } from 'kafka-ts';
import { OpenTelemetryTracer } from './opentelemetry';

setTracer(new OpenTelemetryTracer());

export const kafka = createKafkaClient({
    clientId: 'examples',
    bootstrapServers: [{ host: 'localhost', port: 39092 }, { host: 'localhost', port: 39093 }, { host: 'localhost', port: 39094 }],
    sasl: saslScramSha512({ username: 'admin', password: 'admin' }),
    ssl: { ca: readFileSync('../certs/ca.crt').toString() },
});
