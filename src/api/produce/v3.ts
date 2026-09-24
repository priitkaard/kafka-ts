import { createApi } from '../../utils/api';
import { createBatch, ProduceRequest, ProduceResponse, throwIfError } from './common';

/*
Produce Request (Version: 3) => { transactional_id acks timeout_ms [topic_data] }
  transactional_id => NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => { name [partition_data] }
    name => STRING
    partition_data => { index records }
      index => INT32
      records => NULLABLE_RECORDS

Produce Response (Version: 3) => { [responses] throttle_time_ms }
  responses => { name [partition_responses] }
    name => STRING
    partition_responses => { index error_code base_offset log_append_time_ms }
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
  throttle_time_ms => INT32
*/
export const PRODUCE_V3 = createApi<ProduceRequest, ProduceResponse>({
    apiKey: 0,
    apiVersion: 3,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeString(data.transactionalId)
            .writeInt16(data.acks)
            .writeInt32(data.timeoutMs)
            .writeArray(data.topicData, (encoder, topic) =>
                encoder.writeString(topic.name).writeArray(topic.partitionData, (encoder, partition) => {
                    const batch = createBatch(partition);
                    return encoder.writeInt32(partition.index).writeInt32(batch.getBufferLength()).writeEncoder(batch);
                }),
            ),
    response: (decoder) =>
        throwIfError({
            responses: decoder.readArray((response) => ({
                name: response.readString()!,
                partitionResponses: response.readArray((partitionResponse) => ({
                    index: partitionResponse.readInt32(),
                    errorCode: partitionResponse.readInt16(),
                    baseOffset: partitionResponse.readInt64(),
                    logAppendTime: partitionResponse.readInt64(),
                    logStartOffset: -1n,
                    recordErrors: [],
                    errorMessage: null,
                })),
            })),
            throttleTimeMs: decoder.readInt32(),
        }),
});
