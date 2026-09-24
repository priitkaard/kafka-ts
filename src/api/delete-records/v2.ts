import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DELETE_RECORDS_V1 } from './v1';

/*
DeleteRecords Request (Version: 2) => { (topics) timeout_ms }
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index offset }
      partition_index => INT32
      offset => INT64
  timeout_ms => INT32

DeleteRecords Response (Version: 2) => { throttle_time_ms (topics) }
  throttle_time_ms => INT32
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index low_watermark error_code }
      partition_index => INT32
      low_watermark => INT64
      error_code => INT16
*/
export const DELETE_RECORDS_V2 = createApi({
    ...DELETE_RECORDS_V1,
    apiVersion: 2,
    fallback: DELETE_RECORDS_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder.writeInt32(partition.partitionIndex).writeInt64(partition.offset).writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeInt32(data.timeoutMs)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readCompactArray((topic) => ({
                name: topic.readCompactString()!,
                partitions: topic.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    lowWatermark: partition.readInt64(),
                    errorCode: partition.readInt16(),
                    tags: partition.readTagBuffer(),
                })),
                tags: topic.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
