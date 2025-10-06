export const groupByLeaderId = <T extends { topic: string; partition: number }>(
    items: T[],
    leaderIdByTopicPartition: { [topic: string]: { [partition: number]: number } },
) => {
    const result: { [nodeId: number]: T[] } = {};
    items.forEach((item) => {
        const leaderId = leaderIdByTopicPartition[item.topic][item.partition];
        result[leaderId] ??= [];
        result[leaderId].push(item);
    });
    return result;
};
