import { kafka } from './client';

const producer = kafka.createProducer({ allowTopicAutoCreation: true });

// const rl = createInterface({ input: process.stdin, output: process.stdout });
// process.once('SIGINT', rl.close);

// process.stdout.write('> ');
// rl.on('line', async (line) => {
//     await producer.send(
//         [
//             {
//                 topic: 'my-topic',
//                 value: line,
//             },
//         ],
//         { acks: -1 },
//     );
//     process.stdout.write('> ');
// });
// rl.once('close', () => producer.close());

const send = async () => {
    await producer
        .send(
            [
                {
                    topic: 'my-topic',
                    value: 'ping',
                },
            ],
            { acks: -1 },
        )
        .catch(console.error)
        .finally(() => setTimeout(send, 1000));
};

void send();

process.on('uncaughtException', () => {});
