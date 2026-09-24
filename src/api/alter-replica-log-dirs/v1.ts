import { createApi } from '../../utils/api';
import { AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse, throwIfError } from './common';

/*
AlterReplicaLogDirs Request (Version: 1) => { [dirs] }
  dirs => { path [topics] }
    path => STRING
    topics => { name [partitions] }
      name => STRING
      partitions => INT32

AlterReplicaLogDirs Response (Version: 1) => { throttle_time_ms [results] }
  throttle_time_ms => INT32
  results => { topic_name [partitions] }
    topic_name => STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const ALTER_REPLICA_LOG_DIRS_V1 = createApi<AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse>({
    apiKey: 34,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder.writeArray(data.dirs, (encoder, dir) =>
            encoder
                .writeString(dir.path)
                .writeArray(dir.topics, (encoder, topic) =>
                    encoder
                        .writeString(topic.name)
                        .writeArray(topic.partitions, (encoder, partition) => encoder.writeInt32(partition)),
                ),
        ),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            results: decoder.readArray((resultItem) => ({
                topicName: resultItem.readString()!,
                partitions: resultItem.readArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        }),
});
