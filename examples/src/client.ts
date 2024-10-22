import { readFileSync } from 'fs';
import { createKafkaClient } from 'kafka-ts';

export const kafka = createKafkaClient({
    clientId: 'examples',
    bootstrapServers: [{ host: 'localhost', port: 9092 }],
    sasl: { mechanism: 'SCRAM-SHA-256', username: 'admin', password: 'admin' },
    ssl: { ca: readFileSync('../certs/ca.crt').toString() },
});
