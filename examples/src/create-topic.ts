import { kafka } from "./client";
import { API } from "kafka-ts";

(async () => {
    const cluster = kafka.createCluster();
    await cluster.connect();

    const { controllerId } = await cluster.sendRequest(API.METADATA, {
        allowTopicAutoCreation: false,
        includeTopicAuthorizedOperations: false,
        topics: [],
    });

    await cluster.sendRequestToNode(controllerId)(API.CREATE_TOPICS, {
        validateOnly: false,
        timeoutMs: 10_000,
        topics: [
            {
                name: "my-topic",
                numPartitions: 10,
                replicationFactor: 3,
                assignments: [],
                configs: [],
            },
        ],
    });

    const metadata = await cluster.sendRequestToNode(controllerId)(API.METADATA, {
        allowTopicAutoCreation: false,
        includeTopicAuthorizedOperations: false,
        topics: [{ id: null, name: "my-topic" }],
    });

    console.log(metadata);

    await cluster.disconnect();
})();
