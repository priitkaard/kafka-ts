import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { ALTER_REPLICA_LOG_DIRS_V1 } from './v1';

/*
AlterReplicaLogDirs Request (Version: 2) => { (dirs) }
  dirs => { path (topics) }
    path => COMPACT_STRING
    topics => { name (partitions) }
      name => COMPACT_STRING
      partitions => INT32

AlterReplicaLogDirs Response (Version: 2) => { throttle_time_ms (results) }
  throttle_time_ms => INT32
  results => { topic_name (partitions) }
    topic_name => COMPACT_STRING
    partitions => { partition_index error_code }
      partition_index => INT32
      error_code => INT16
*/
export const ALTER_REPLICA_LOG_DIRS_V2 = createApi({
    ...ALTER_REPLICA_LOG_DIRS_V1,
    apiVersion: 2,
    fallback: ALTER_REPLICA_LOG_DIRS_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.dirs, (encoder, dir) =>
                encoder
                    .writeCompactString(dir.path)
                    .writeCompactArray(dir.topics, (encoder, topic) =>
                        encoder
                            .writeCompactString(topic.name)
                            .writeCompactArray(topic.partitions, (encoder, partition) => encoder.writeInt32(partition))
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            results: decoder.readCompactArray((resultItem) => ({
                topicName: resultItem.readCompactString()!,
                partitions: resultItem.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    tags: partition.readTagBuffer(),
                })),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
