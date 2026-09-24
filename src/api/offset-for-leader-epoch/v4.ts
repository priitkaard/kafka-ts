import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { OFFSET_FOR_LEADER_EPOCH_V3 } from './v3';

/*
OffsetForLeaderEpoch Request (Version: 4) => { replica_id (topics) }
  replica_id => INT32
  topics => { topic (partitions) }
    topic => COMPACT_STRING
    partitions => { partition current_leader_epoch leader_epoch }
      partition => INT32
      current_leader_epoch => INT32
      leader_epoch => INT32

OffsetForLeaderEpoch Response (Version: 4) => { throttle_time_ms (topics) }
  throttle_time_ms => INT32
  topics => { topic (partitions) }
    topic => COMPACT_STRING
    partitions => { error_code partition leader_epoch end_offset }
      error_code => INT16
      partition => INT32
      leader_epoch => INT32
      end_offset => INT64
*/
export const OFFSET_FOR_LEADER_EPOCH_V4 = createApi({
    ...OFFSET_FOR_LEADER_EPOCH_V3,
    apiVersion: 4,
    fallback: OFFSET_FOR_LEADER_EPOCH_V3,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeInt32(data.replicaId ?? -2)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.topic)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partition)
                            .writeInt32(partition.currentLeaderEpoch)
                            .writeInt32(partition.leaderEpoch)
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readCompactArray((topic) => ({
                topic: topic.readCompactString()!,
                partitions: topic.readCompactArray((partition) => ({
                    errorCode: partition.readInt16(),
                    partition: partition.readInt32(),
                    leaderEpoch: partition.readInt32(),
                    endOffset: partition.readInt64(),
                    tags: partition.readTagBuffer(),
                })),
                tags: topic.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
