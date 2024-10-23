import { Metadata } from '../metadata';
import { Message } from '../types';
import { murmur2, toPositive } from '../utils/murmur2';

export type Partition = (message: Message) => number;
export type Partitioner = (context: { metadata: Metadata }) => Partition;

export const defaultPartitioner: Partitioner = ({ metadata }) => {
    const topicCounterMap: Record<string, number> = {};

    const getNextValue = (topic: string) => {
        topicCounterMap[topic] ??= 0;
        return topicCounterMap[topic]++;
    };

    return ({ topic, partition, key }: Message) => {
        if (partition !== null && partition !== undefined) {
            return partition;
        }
        const partitions = metadata.getTopicPartitions()[topic];
        const numPartitions = partitions.length;
        if (key) {
            return toPositive(murmur2(key)) % numPartitions;
        }
        return toPositive(getNextValue(topic)) % numPartitions;
    };
};
