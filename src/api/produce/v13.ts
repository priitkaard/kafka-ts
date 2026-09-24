import { createApi } from '../../utils/api';
import { createBatch, throwIfError } from './common';
import { PRODUCE_V12 } from './v12';

/*
Produce Request (Version: 13) => { transactional_id acks timeout_ms (topic_data) }
  transactional_id => COMPACT_NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => { topic_id (partition_data) }
    topic_id => UUID
    partition_data => { index records }
      index => INT32
      records => COMPACT_NULLABLE_RECORDS

Produce Response (Version: 13) => { (responses) throttle_time_ms node_endpoints<tag: 0> }
  responses => { topic_id (partition_responses) }
    topic_id => UUID
    partition_responses => { index error_code base_offset log_append_time_ms log_start_offset (record_errors) error_message current_leader<tag: 0> }
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
      record_errors => { batch_index batch_index_error_message }
        batch_index => INT32
        batch_index_error_message => COMPACT_NULLABLE_STRING
      error_message => COMPACT_NULLABLE_STRING
      current_leader<tag: 0> => { leader_id leader_epoch }
        leader_id => INT32
        leader_epoch => INT32
  throttle_time_ms => INT32
  node_endpoints<tag: 0> => { node_id host port rack }
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
*/
export const PRODUCE_V13 = createApi({
    ...PRODUCE_V12,
    apiVersion: 13,
    fallback: PRODUCE_V12,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.transactionalId)
            .writeInt16(data.acks)
            .writeInt32(data.timeoutMs)
            .writeCompactArray(data.topicData, (encoder, topic) =>
                encoder
                    .writeUUID(topic.topicId)
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
                topicId: response.readUUID(),
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
