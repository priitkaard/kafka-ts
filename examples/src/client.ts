import { readFileSync } from 'fs';
import { createKafkaClient, saslScramSha512 } from 'kafka-ts';

export const kafka = createKafkaClient({
    clientId: 'examples',
    bootstrapServers: [{ host: 'localhost', port: 9092 }],
    sasl: saslScramSha512({ username: 'admin', password: 'admin' }),
    ssl: { ca: readFileSync('../certs/ca.crt').toString() },
});
