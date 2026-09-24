import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_LOG_DIRS_V4 } from './v4';

/*
DescribeLogDirs Request (Version: 5) => { ?(topics) }
  topics => { topic (partitions) }
    topic => COMPACT_STRING
    partitions => INT32

DescribeLogDirs Response (Version: 5) => { throttle_time_ms error_code (results) }
  throttle_time_ms => INT32
  error_code => INT16
  results => { error_code log_dir (topics) total_bytes usable_bytes is_cordoned }
    error_code => INT16
    log_dir => COMPACT_STRING
    topics => { name (partitions) }
      name => COMPACT_STRING
      partitions => { partition_index partition_size offset_lag is_future_key }
        partition_index => INT32
        partition_size => INT64
        offset_lag => INT64
        is_future_key => BOOLEAN
    total_bytes => INT64
    usable_bytes => INT64
    is_cordoned => BOOLEAN
*/
export const DESCRIBE_LOG_DIRS_V5 = createApi({
    ...DESCRIBE_LOG_DIRS_V4,
    apiVersion: 5,
    fallback: DESCRIBE_LOG_DIRS_V4,
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
                totalBytes: resultItem.readInt64(),
                usableBytes: resultItem.readInt64(),
                isCordoned: resultItem.readBoolean(),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
