import { createApi } from '../../utils/api';
import { DescribeLogDirsRequest, DescribeLogDirsResponse, throwIfError } from './common';

/*
DescribeLogDirs Request (Version: 1) => { ?[topics] }
  topics => { topic [partitions] }
    topic => STRING
    partitions => INT32

DescribeLogDirs Response (Version: 1) => { throttle_time_ms [results] }
  throttle_time_ms => INT32
  results => { error_code log_dir [topics] }
    error_code => INT16
    log_dir => STRING
    topics => { name [partitions] }
      name => STRING
      partitions => { partition_index partition_size offset_lag is_future_key }
        partition_index => INT32
        partition_size => INT64
        offset_lag => INT64
        is_future_key => BOOLEAN
*/
export const DESCRIBE_LOG_DIRS_V1 = createApi<DescribeLogDirsRequest, DescribeLogDirsResponse>({
    apiKey: 35,
    apiVersion: 1,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder.writeArray(data.topics, (encoder, topic) =>
            encoder
                .writeString(topic.topic)
                .writeArray(topic.partitions, (encoder, partition) => encoder.writeInt32(partition)),
        ),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: 0,
            results: decoder.readArray((resultItem) => ({
                errorCode: resultItem.readInt16(),
                logDir: resultItem.readString()!,
                topics: resultItem.readArray((topic) => ({
                    name: topic.readString()!,
                    partitions: topic.readArray((partition) => ({
                        partitionIndex: partition.readInt32(),
                        partitionSize: partition.readInt64(),
                        offsetLag: partition.readInt64(),
                        isFutureKey: partition.readBoolean(),
                        tags: {},
                    })),
                    tags: {},
                })),
                totalBytes: -1n,
                usableBytes: -1n,
                isCordoned: false,
                tags: {},
            })),
            tags: {},
        }),
});
