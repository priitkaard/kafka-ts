import { createApi } from '../../utils/api';
import { DeleteRecordsRequest, DeleteRecordsResponse, throwIfError } from './common';

/*
DeleteRecords Request (Version: 0) => { [topics] timeout_ms }
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index offset }
      partition_index => INT32
      offset => INT64
  timeout_ms => INT32

DeleteRecords Response (Version: 0) => { throttle_time_ms [topics] }
  throttle_time_ms => INT32
  topics => { name [partitions] }
    name => STRING
    partitions => { partition_index low_watermark error_code }
      partition_index => INT32
      low_watermark => INT64
      error_code => INT16
*/
export const DELETE_RECORDS_V0 = createApi<DeleteRecordsRequest, DeleteRecordsResponse>({
    apiKey: 21,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeArray(data.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.name)
                    .writeArray(topic.partitions, (encoder, partition) =>
                        encoder.writeInt32(partition.partitionIndex).writeInt64(partition.offset),
                    ),
            )
            .writeInt32(data.timeoutMs),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readArray((topic) => ({
                name: topic.readString()!,
                partitions: topic.readArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    lowWatermark: partition.readInt64(),
                    errorCode: partition.readInt16(),
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        }),
});
