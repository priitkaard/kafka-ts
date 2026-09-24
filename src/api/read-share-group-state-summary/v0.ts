import { createApi } from '../../utils/api';
import { ReadShareGroupStateSummaryRequest, ReadShareGroupStateSummaryResponse, throwIfError } from './common';

/*
ReadShareGroupStateSummary Request (Version: 0) => { group_id (topics) }
  group_id => COMPACT_STRING
  topics => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition leader_epoch }
      partition => INT32
      leader_epoch => INT32

ReadShareGroupStateSummary Response (Version: 0) => { (results) }
  results => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition error_code error_message state_epoch leader_epoch start_offset }
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      state_epoch => INT32
      leader_epoch => INT32
      start_offset => INT64
*/
export const READ_SHARE_GROUP_STATE_SUMMARY_V0 = createApi<
    ReadShareGroupStateSummaryRequest,
    ReadShareGroupStateSummaryResponse
>({
    apiKey: 87,
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
                    leaderEpoch: partition.readInt32(),
                    startOffset: partition.readInt64(),
                    deliveryCompleteCount: -1,
                    tags: partition.readTagBuffer(),
                })),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
