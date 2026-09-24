import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { PRODUCE_V4 } from './v4';

/*
Produce Request (Version: 5) => { transactional_id acks timeout_ms [topic_data] }
  transactional_id => NULLABLE_STRING
  acks => INT16
  timeout_ms => INT32
  topic_data => { name [partition_data] }
    name => STRING
    partition_data => { index records }
      index => INT32
      records => NULLABLE_RECORDS

Produce Response (Version: 5) => { [responses] throttle_time_ms }
  responses => { name [partition_responses] }
    name => STRING
    partition_responses => { index error_code base_offset log_append_time_ms log_start_offset }
      index => INT32
      error_code => INT16
      base_offset => INT64
      log_append_time_ms => INT64
      log_start_offset => INT64
  throttle_time_ms => INT32
*/
export const PRODUCE_V5 = createApi({
    ...PRODUCE_V4,
    apiVersion: 5,
    fallback: PRODUCE_V4,
    response: (decoder) =>
        throwIfError({
            responses: decoder.readArray((response) => ({
                name: response.readString()!,
                partitionResponses: response.readArray((partitionResponse) => ({
                    index: partitionResponse.readInt32(),
                    errorCode: partitionResponse.readInt16(),
                    baseOffset: partitionResponse.readInt64(),
                    logAppendTime: partitionResponse.readInt64(),
                    logStartOffset: partitionResponse.readInt64(),
                    recordErrors: [],
                    errorMessage: null,
                })),
            })),
            throttleTimeMs: decoder.readInt32(),
        }),
});
