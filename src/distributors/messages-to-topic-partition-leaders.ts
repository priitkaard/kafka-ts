type TopicPartitionLeader = { [topicName: string]: { [partitionId: number]: number } };
type MessagesByNodeTopicPartition<T> = {
    [nodeId: number]: { [topicName: string]: { [partitionId: number]: T[] } };
};

export const distributeMessagesToTopicPartitionLeaders = <T extends { topic: string; partition: number }>(
    messages: T[],
    topicPartitionLeader: TopicPartitionLeader,
) => {
    const result: MessagesByNodeTopicPartition<T> = {};
    messages.forEach((message) => {
        const leaderId = topicPartitionLeader[message.topic][message.partition];
        result[leaderId] ??= {};
        result[leaderId][message.topic] ??= {};
        result[leaderId][message.topic][message.partition] ??= [];
        result[leaderId][message.topic][message.partition].push(message);
    });
    return result;
};
