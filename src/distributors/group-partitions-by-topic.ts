export const groupPartitionsByTopic = <T extends { topic: string; partition: number }>(items: T[]) => {
    const result: { [topic: string]: number[] } = {};
    items.forEach((item) => {
        result[item.topic] ??= [];
        result[item.topic].push(item.partition);
    });
    return result;
};
