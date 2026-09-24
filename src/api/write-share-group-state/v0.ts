import { createApi } from '../../utils/api';
import { throwIfError, WriteShareGroupStateRequest, WriteShareGroupStateResponse } from './common';

/*
WriteShareGroupState Request (Version: 0) => { group_id (topics) }
  group_id => COMPACT_STRING
  topics => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition state_epoch leader_epoch start_offset (state_batches) }
      partition => INT32
      state_epoch => INT32
      leader_epoch => INT32
      start_offset => INT64
      state_batches => { first_offset last_offset delivery_state delivery_count }
        first_offset => INT64
        last_offset => INT64
        delivery_state => INT8
        delivery_count => INT16

WriteShareGroupState Response (Version: 0) => { (results) }
  results => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition error_code error_message }
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
*/
export const WRITE_SHARE_GROUP_STATE_V0 = createApi<WriteShareGroupStateRequest, WriteShareGroupStateResponse>({
    apiKey: 85,
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
                        encoder
                            .writeInt32(partition.partition)
                            .writeInt32(partition.stateEpoch)
                            .writeInt32(partition.leaderEpoch)
                            .writeInt64(partition.startOffset)
                            .writeCompactArray(partition.stateBatches, (encoder, stateBatche) =>
                                encoder
                                    .writeInt64(stateBatche.firstOffset)
                                    .writeInt64(stateBatche.lastOffset)
                                    .writeInt8(stateBatche.deliveryState)
                                    .writeInt16(stateBatche.deliveryCount)
                                    .writeTagBuffer(),
                            )
                            .writeTagBuffer(),
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
