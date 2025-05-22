import { readFileSync } from 'fs';
import { createKafkaClient, LogLevel, saslScramSha512, setLogLevel } from 'kafka-ts';

// setTracer(new OpenTelemetryTracer());

setLogLevel(LogLevel.DEBUG);

export const kafka = createKafkaClient({
    clientId: 'examples',
    bootstrapServers: [
        { host: 'localhost', port: 39092 },
        { host: 'localhost', port: 39093 },
        { host: 'localhost', port: 39094 },
    ],
    sasl: saslScramSha512({ username: 'admin', password: 'admin' }),
    ssl: { ca: readFileSync('../certs/ca.crt').toString() },
});
