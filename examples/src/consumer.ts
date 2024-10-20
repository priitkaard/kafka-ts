import { kafka } from "./client";

(async () => {
    const consumer = await kafka.startConsumer({
        groupId: "example-group",
        groupInstanceId: "example-group-instance",
        topics: ["example-topic-f"],
        allowTopicAutoCreation: true,
        onMessage: (message) => {
            console.log(message);
        },
    });

    process.on("SIGINT", async () => {
        await consumer.close();
    });
})();
