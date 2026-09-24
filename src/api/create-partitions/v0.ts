import { createApi } from '../../utils/api';
import { CreatePartitionsRequest, CreatePartitionsResponse, throwIfError } from './common';

/*
CreatePartitions Request (Version: 0) => { [topics] timeout_ms validate_only }
  topics => { name count ?[assignments] }
    name => STRING
    count => INT32
    assignments => { [broker_ids] }
      broker_ids => INT32
  timeout_ms => INT32
  validate_only => BOOLEAN

CreatePartitions Response (Version: 0) => { throttle_time_ms [results] }
  throttle_time_ms => INT32
  results => { name error_code error_message }
    name => STRING
    error_code => INT16
    error_message => NULLABLE_STRING
*/
export const CREATE_PARTITIONS_V0 = createApi<CreatePartitionsRequest, CreatePartitionsResponse>({
    apiKey: 37,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeArray(data.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.name)
                    .writeInt32(topic.count)
                    .writeArray(topic.assignments, (encoder, assignment) =>
                        encoder.writeArray(assignment.brokerIds, (encoder, brokerId) => encoder.writeInt32(brokerId)),
                    ),
            )
            .writeInt32(data.timeoutMs)
            .writeBoolean(data.validateOnly),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            results: decoder.readArray((resultItem) => ({
                name: resultItem.readString()!,
                errorCode: resultItem.readInt16(),
                errorMessage: resultItem.readString(),
                tags: {},
            })),
            tags: {},
        }),
});
