import { createApi } from '../../utils/api';
import { createBatch, throwIfError } from './common';
import { PRODUCE_V8 } from './v8';

/*
Produce Request (Version: 9) => { transactional_id acks timeout_ms (topic_data) }
  transactional_id => COMPACT_NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => { name (partition_data) }
    name => COMPACT_STRING
    partition_data => { index records }
      index => INT32
      records => COMPACT_NULLABLE_RECORDS

Produce Response (Version: 9) => { (responses) throttle_time_ms }
  responses => { name (partition_responses) }
    name => COMPACT_STRING
    partition_responses => { index error_code base_offset log_append_time_ms log_start_offset (record_errors) error_message }
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
      record_errors => { batch_index batch_index_error_message }
        batch_index => INT32
        batch_index_error_message => COMPACT_NULLABLE_STRING
      error_message => COMPACT_NULLABLE_STRING
  throttle_time_ms => INT32
*/
export const PRODUCE_V9 = createApi({
    ...PRODUCE_V8,
    apiVersion: 9,
    fallback: PRODUCE_V8,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.transactionalId)
            .writeInt16(data.acks)
            .writeInt32(data.timeoutMs)
            .writeCompactArray(data.topicData, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitionData, (encoder, partition) => {
                        const batch = createBatch(partition);
                        return encoder
                            .writeInt32(partition.index)
                            .writeUVarInt(batch.getBufferLength() + 1)
                            .writeEncoder(batch)
                            .writeTagBuffer();
                    })
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            responses: decoder.readCompactArray((response) => ({
                name: response.readCompactString()!,
                partitionResponses: response.readCompactArray((partitionResponse) => ({
                    index: partitionResponse.readInt32(),
                    errorCode: partitionResponse.readInt16(),
                    baseOffset: partitionResponse.readInt64(),
                    logAppendTime: partitionResponse.readInt64(),
                    logStartOffset: partitionResponse.readInt64(),
                    recordErrors: partitionResponse.readCompactArray((recordError) => ({
                        batchIndex: recordError.readInt32(),
                        batchIndexErrorMessage: recordError.readCompactString(),
                        tags: recordError.readTagBuffer(),
                    })),
                    errorMessage: partitionResponse.readCompactString(),
                    tags: partitionResponse.readTagBuffer(),
                })),
                tags: response.readTagBuffer(),
            })),
            throttleTimeMs: decoder.readInt32(),
            tags: decoder.readTagBuffer(),
        }),
});
