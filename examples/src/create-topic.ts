import { API, API_ERROR, KafkaTSApiError, log } from 'kafka-ts';
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
                    replicationFactor: 1,
                    assignments: [],
                    configs: [],
                },
            ],
        });
    } catch (error) {
        if ((error as KafkaTSApiError).errorCode !== API_ERROR.TOPIC_ALREADY_EXISTS) {
            throw error;
        }
    }

    const metadata = await cluster.sendRequestToNode(controllerId)(API.METADATA, {
        allowTopicAutoCreation: false,
        includeTopicAuthorizedOperations: false,
        topics: [{ id: null, name: 'my-topic' }],
    });

    log.info('Metadata', metadata);

    await cluster.disconnect();
})();
