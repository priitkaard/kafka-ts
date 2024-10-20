import { createInterface } from "readline";
import { kafka } from "./client";

const producer = kafka.createProducer({ allowTopicAutoCreation: true });

const rl = createInterface({ input: process.stdin, output: process.stdout });

process.stdout.write("> ");
rl.on("line", async (line) => {
    await producer.send([
        {
            topic: "example-topic-f",
            key: null,
            value: line,
            partition: 0,
        },
    ]);
    process.stdout.write("> ");
});

process.on("SIGINT", async () => {
    rl.close();
    await producer.close();
});
