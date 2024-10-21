type Assignment = { [topicName: string]: number[] };
type TopicPartitionReplicaIds = { [topicName: string]: { [partition: number]: number[] } };
export type NodeAssignment = { [replicaId: number]: Assignment };

/** From replica ids pick the one with fewest assignments to balance the load across brokers */
export const distributeAssignmentsToNodesBalanced = (
    assignment: Assignment,
    topicPartitionReplicaIds: TopicPartitionReplicaIds,
) => {
    const replicaPartitions = getPartitionsByReplica(assignment, topicPartitionReplicaIds);

    const result: NodeAssignment = {};

    for (const [topicName, partitions] of Object.entries(assignment)) {
        for (const partition of partitions) {
            const replicaIds = topicPartitionReplicaIds[topicName][partition];
            const replicaId = replicaIds.reduce((prev, curr) => {
                if (!prev) {
                    return curr;
                }
                return (replicaPartitions[prev]?.length ?? 0) < (replicaPartitions[curr]?.length ?? 0) ? prev : curr;
            });
            result[replicaId] ??= {};
            result[replicaId][topicName] ??= [];
            result[replicaId][topicName].push(partition);
        }
    }

    return result;
};

/** Minimize the total number of replicas in the result to reduce the number of requests to different brokers */
export const distributeAssignmentsToNodesOptimized = (
    assignment: Assignment,
    topicPartitionReplicaIds: TopicPartitionReplicaIds,
) => {
    const result: NodeAssignment = {};

    const sortFn = ([, partitionsA]: [string, string[]], [, partitionsB]: [string, string[]]) =>
        partitionsB.length - partitionsA.length;

    let replicaPartitions = getPartitionsByReplica(assignment, topicPartitionReplicaIds);

    while (replicaPartitions.length) {
        replicaPartitions.sort(sortFn);

        const [replicaId, partitions] = replicaPartitions.shift()!;
        if (!partitions.length) {
            continue;
        }

        result[parseInt(replicaId)] = partitions.reduce((acc, partition) => {
            const [topicName, partitionId] = partition.split(':');
            acc[topicName] ??= [];
            acc[topicName].push(parseInt(partitionId));
            return acc;
        }, {} as Assignment);

        replicaPartitions = replicaPartitions.map(
            ([replicaId, replicaPartitions]) =>
                [replicaId, replicaPartitions.filter((partition) => !partitions.includes(partition))] as [
                    string,
                    string[],
                ],
        );
    }

    return result;
};

const getPartitionsByReplica = (assignment: Assignment, topicPartitionReplicaIds: TopicPartitionReplicaIds) => {
    const partitionsByReplicaId: { [replicaId: number]: string[] } = {};
    for (const [topicName, partitions] of Object.entries(assignment)) {
        for (const partition of partitions) {
            const replicaIds = topicPartitionReplicaIds[topicName][partition];
            for (const replicaId of replicaIds) {
                partitionsByReplicaId[replicaId] ??= [];
                partitionsByReplicaId[replicaId].push(`${topicName}:${partition}`);
            }
        }
    }
    return Object.entries(partitionsByReplicaId);
};

export const distributeAssignmentsToNodes = distributeAssignmentsToNodesBalanced;
