import { createApi } from '../../utils/api';
import { DeleteShareGroupStateRequest, DeleteShareGroupStateResponse, throwIfError } from './common';

/*
DeleteShareGroupState Request (Version: 0) => { group_id (topics) }
  group_id => COMPACT_STRING
  topics => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition }
      partition => INT32

DeleteShareGroupState Response (Version: 0) => { (results) }
  results => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition error_code error_message }
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
*/
export const DELETE_SHARE_GROUP_STATE_V0 = createApi<DeleteShareGroupStateRequest, DeleteShareGroupStateResponse>({
    apiKey: 86,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeUUID(topic.topicId)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder.writeInt32(partition.partition).writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            results: decoder.readCompactArray((resultItem) => ({
                topicId: resultItem.readUUID(),
                partitions: resultItem.readCompactArray((partition) => ({
                    partition: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    errorMessage: partition.readCompactString(),
                    tags: partition.readTagBuffer(),
                })),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
