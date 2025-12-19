import { readFileSync } from 'fs';
import { createKafkaClient, LogLevel, oAuthAuthenticator, oAuthBearer, saslScramSha512, setLogLevel } from 'kafka-ts';

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
    // sasl: oAuthBearer(
    //     oAuthAuthenticator({
    //         endpoint: 'https://oauth.example.com/token',
    //         clientId: 'my-client-id',
    //         clientSecret: 'my-client-secret',
    //     }),
    // ),
    ssl: { ca: readFileSync('../certs/ca.crt').toString() },
});
