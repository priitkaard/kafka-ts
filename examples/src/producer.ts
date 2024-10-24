import { createInterface } from 'readline';
import { kafka } from './client';

const producer = kafka.createProducer({ allowTopicAutoCreation: true });

const rl = createInterface({ input: process.stdin, output: process.stdout });

process.stdout.write('> ');
rl.on('line', async (line) => {
    await producer.send([
        {
            topic: 'my-topic',
            value: Buffer.from(line),
        },
    ], { acks: -1 });
    process.stdout.write('> ');
});

process.on('SIGINT', async () => {
    rl.close();
    await producer.close();
});
