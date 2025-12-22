import { createApi } from '../utils/api';
import { KafkaTSApiError } from '../utils/error';
import { IsolationLevel } from './fetch';

type ListOffsetsRequest = {
    replicaId: number;
    isolationLevel: IsolationLevel;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            currentLeaderEpoch: number;
            timestamp: bigint;
        }[];
    }[];
};

type ListOffsetsResponse = {
    throttleTimeMs: number;
    topics: {
        name: string;
        partitions: {
            partitionIndex: number;
            errorCode: number;
            timestamp: bigint;
            offset: bigint;
            leaderEpoch: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

/*
ListOffsets Request (Version: 1) => replica_id [topics] 
  replica_id => INT32
  topics => name [partitions] 
    name => STRING
    partitions => partition_index timestamp 
      partition_index => INT32
      timestamp => INT64

ListOffsets Response (Version: 1) => [topics] 
  topics => name [partitions] 
    name => STRING
    partitions => partition_index error_code timestamp offset 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
*/
const LIST_OFFSETS_V1 = createApi<ListOffsetsRequest, ListOffsetsResponse>({
    apiKey: 2,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeInt32(data.replicaId)
            .writeArray(data.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.name)
                    .writeArray(topic.partitions, (encoder, partition) =>
                        encoder.writeInt32(partition.partitionIndex).writeInt64(partition.timestamp),
                    ),
            ),
    response: (decoder) => {
        const result = {
            throttleTimeMs: 0,
            topics: decoder.readArray((decoder) => ({
                name: decoder.readString()!,
                partitions: decoder.readArray((decoder) => ({
                    partitionIndex: decoder.readInt32(),
                    errorCode: decoder.readInt16(),
                    timestamp: decoder.readInt64(),
                    offset: decoder.readInt64(),
                    leaderEpoch: -1,
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        };
        result.topics.forEach((topic) => {
            topic.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
            });
        });
        return result;
    },
});

/*
ListOffsets Request (Version: 8) => replica_id isolation_level [topics] _tagged_fields 
  replica_id => INT32
  isolation_level => INT8
  topics => name [partitions] _tagged_fields 
    name => COMPACT_STRING
    partitions => partition_index current_leader_epoch timestamp _tagged_fields 
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64

ListOffsets Response (Version: 8) => throttle_time_ms [topics] _tagged_fields 
  throttle_time_ms => INT32
  topics => name [partitions] _tagged_fields 
    name => COMPACT_STRING
    partitions => partition_index error_code timestamp offset leader_epoch _tagged_fields 
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
      leader_epoch => INT32
*/
export const LIST_OFFSETS = createApi<ListOffsetsRequest, ListOffsetsResponse>({
    apiKey: 2,
    apiVersion: 8,
    fallback: LIST_OFFSETS_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeInt32(data.replicaId)
            .writeInt8(data.isolationLevel)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeInt32(partition.currentLeaderEpoch)
                            .writeInt64(partition.timestamp)
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readCompactArray((decoder) => ({
                name: decoder.readCompactString()!,
                partitions: decoder.readCompactArray((decoder) => ({
                    partitionIndex: decoder.readInt32(),
                    errorCode: decoder.readInt16(),
                    timestamp: decoder.readInt64(),
                    offset: decoder.readInt64(),
                    leaderEpoch: decoder.readInt32(),
                    tags: decoder.readTagBuffer(),
                })),
                tags: decoder.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        };
        result.topics.forEach((topic) => {
            topic.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
            });
        });
        return result;
    },
});
