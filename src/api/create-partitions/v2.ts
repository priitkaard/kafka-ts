import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { CREATE_PARTITIONS_V1 } from './v1';

/*
CreatePartitions Request (Version: 2) => { (topics) timeout_ms validate_only }
  topics => { name count ?(assignments) }
    name => COMPACT_STRING
    count => INT32
    assignments => { (broker_ids) }
      broker_ids => INT32
  timeout_ms => INT32
  validate_only => BOOLEAN

CreatePartitions Response (Version: 2) => { throttle_time_ms (results) }
  throttle_time_ms => INT32
  results => { name error_code error_message }
    name => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const CREATE_PARTITIONS_V2 = createApi({
    ...CREATE_PARTITIONS_V1,
    apiVersion: 2,
    fallback: CREATE_PARTITIONS_V1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeInt32(topic.count)
                    .writeCompactArray(topic.assignments, (encoder, assignment) =>
                        encoder
                            .writeCompactArray(assignment.brokerIds, (encoder, brokerId) =>
                                encoder.writeInt32(brokerId),
                            )
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeInt32(data.timeoutMs)
            .writeBoolean(data.validateOnly)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            results: decoder.readCompactArray((resultItem) => ({
                name: resultItem.readCompactString()!,
                errorCode: resultItem.readInt16(),
                errorMessage: resultItem.readCompactString(),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
