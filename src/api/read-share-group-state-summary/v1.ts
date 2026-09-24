import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { READ_SHARE_GROUP_STATE_SUMMARY_V0 } from './v0';

/*
ReadShareGroupStateSummary Request (Version: 1) => { group_id (topics) }
  group_id => COMPACT_STRING
  topics => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition leader_epoch }
      partition => INT32
      leader_epoch => INT32

ReadShareGroupStateSummary Response (Version: 1) => { (results) }
  results => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition error_code error_message state_epoch leader_epoch start_offset delivery_complete_count }
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      state_epoch => INT32
      leader_epoch => INT32
      start_offset => INT64
      delivery_complete_count => INT32
*/
export const READ_SHARE_GROUP_STATE_SUMMARY_V1 = createApi({
    ...READ_SHARE_GROUP_STATE_SUMMARY_V0,
    apiVersion: 1,
    fallback: READ_SHARE_GROUP_STATE_SUMMARY_V0,
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
                    deliveryCompleteCount: partition.readInt32(),
                    tags: partition.readTagBuffer(),
                })),
                tags: resultItem.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
