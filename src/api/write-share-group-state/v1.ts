import { createApi } from '../../utils/api';
import { WRITE_SHARE_GROUP_STATE_V0 } from './v0';

/*
WriteShareGroupState Request (Version: 1) => { group_id (topics) }
  group_id => COMPACT_STRING
  topics => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition state_epoch leader_epoch start_offset delivery_complete_count (state_batches) }
      partition => INT32
      state_epoch => INT32
      leader_epoch => INT32
      start_offset => INT64
      delivery_complete_count => INT32
      state_batches => { first_offset last_offset delivery_state delivery_count }
        first_offset => INT64
        last_offset => INT64
        delivery_state => INT8
        delivery_count => INT16

WriteShareGroupState Response (Version: 1) => { (results) }
  results => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition error_code error_message }
      partition => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
*/
export const WRITE_SHARE_GROUP_STATE_V1 = createApi({
    ...WRITE_SHARE_GROUP_STATE_V0,
    apiVersion: 1,
    fallback: WRITE_SHARE_GROUP_STATE_V0,
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
                            .writeInt32(partition.deliveryCompleteCount ?? -1)
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
});
