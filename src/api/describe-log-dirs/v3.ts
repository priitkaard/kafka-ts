import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_LOG_DIRS_V2 } from './v2';

/*
DescribeLogDirs Request (Version: 3) => { ?(topics) }
  topics => { topic (partitions) }
    topic => COMPACT_STRING
    partitions => INT32

DescribeLogDirs Response (Version: 3) => { throttle_time_ms error_code (results) }
  throttle_time_ms => INT32
  error_code => INT16
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
export const DESCRIBE_LOG_DIRS_V3 = createApi({
    ...DESCRIBE_LOG_DIRS_V2,
    apiVersion: 3,
    fallback: DESCRIBE_LOG_DIRS_V2,
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
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
