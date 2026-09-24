import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_LOG_DIRS_V1 } from './v1';

/*
DescribeLogDirs Request (Version: 2) => { ?(topics) }
  topics => { topic (partitions) }
    topic => COMPACT_STRING
    partitions => INT32

DescribeLogDirs Response (Version: 2) => { throttle_time_ms (results) }
  throttle_time_ms => INT32
  results => { error_code log_dir (topics) }
    error_code => INT16
    log_dir => COMPACT_STRING
    topics => { name (partitions) }
      name => COMPACT_STRING
      partitions => { partition_index partition_size offset_lag is_future_key }
        partition_index => INT32
        partition_size => INT64
        offset_lag => INT64
        is_future_key => BOOLEAN
*/
export const DESCRIBE_LOG_DIRS_V2 = createApi({
    ...DESCRIBE_LOG_DIRS_V1,
    apiVersion: 2,
    fallback: DESCRIBE_LOG_DIRS_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.topic)
                    .writeCompactArray(topic.partitions, (encoder, partition) => encoder.writeInt32(partition))
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: 0,
            results: decoder.readCompactArray((resultItem) => ({
                errorCode: resultItem.readInt16(),
                logDir: resultItem.readCompactString()!,
                topics: resultItem.readCompactArray((topic) => ({
                    name: topic.readCompactString()!,
                    partitions: topic.readCompactArray((partition) => ({
                        partitionIndex: partition.readInt32(),
                        partitionSize: partition.readInt64(),
                        offsetLag: partition.readInt64(),
                        isFutureKey: partition.readBoolean(),
                        tags: partition.readTagBuffer(),
                    })),
                    tags: topic.readTagBuffer(),
                })),
                totalBytes: -1n,
                usableBytes: -1n,
                isCordoned: false,
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
