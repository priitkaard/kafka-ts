import { API, API_ERROR, KafkaTSApiError } from 'kafka-ts';
import { kafka } from './client';

(async () => {
    const cluster = kafka.createCluster();
    await cluster.connect();

    const { controllerId } = await cluster.sendRequest(API.METADATA, {
        allowTopicAutoCreation: false,
        includeTopicAuthorizedOperations: false,
        topics: [],
    });

    try {
        await cluster.sendRequestToNode(controllerId)(API.CREATE_TOPICS, {
            validateOnly: false,
            timeoutMs: 10_000,
            topics: [
                {
                    name: 'my-topic',
                    numPartitions: 10,
                    replicationFactor: 3,
                    assignments: [],
                    configs: [],
                },
            ],
        });
    } catch (error) {
        if ((error as KafkaTSApiError).errorCode === API_ERROR.TOPIC_ALREADY_EXISTS) {
            return;
        }
        throw error;
    }

    const metadata = await cluster.sendRequestToNode(controllerId)(API.METADATA, {
        allowTopicAutoCreation: false,
        includeTopicAuthorizedOperations: false,
        topics: [{ id: null, name: 'my-topic' }],
    });

    console.log(metadata);

    await cluster.disconnect();
})();
