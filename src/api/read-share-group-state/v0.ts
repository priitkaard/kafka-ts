import { createApi } from '../../utils/api';
import { ReadShareGroupStateRequest, ReadShareGroupStateResponse, throwIfError } from './common';

/*
ReadShareGroupState Request (Version: 0) => { group_id (topics) }
  group_id => COMPACT_STRING
  topics => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition leader_epoch }
      partition => INT32
      leader_epoch => INT32

ReadShareGroupState Response (Version: 0) => { (results) }
  results => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition error_code error_message state_epoch start_offset (state_batches) }
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      state_epoch => INT32
      start_offset => INT64
      state_batches => { first_offset last_offset delivery_state delivery_count }
        first_offset => INT64
        last_offset => INT64
        delivery_state => INT8
        delivery_count => INT16
*/
export const READ_SHARE_GROUP_STATE_V0 = createApi<ReadShareGroupStateRequest, ReadShareGroupStateResponse>({
    apiKey: 84,
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
                        encoder.writeInt32(partition.partition).writeInt32(partition.leaderEpoch).writeTagBuffer(),
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
                    stateEpoch: partition.readInt32(),
                    startOffset: partition.readInt64(),
                    stateBatches: partition.readCompactArray((stateBatche) => ({
                        firstOffset: stateBatche.readInt64(),
                        lastOffset: stateBatche.readInt64(),
                        deliveryState: stateBatche.readInt8(),
                        deliveryCount: stateBatche.readInt16(),
                        tags: stateBatche.readTagBuffer(),
                    })),
                    tags: partition.readTagBuffer(),
                })),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
