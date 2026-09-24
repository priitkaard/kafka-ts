import { createApi } from '../../utils/api';
import { OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse, throwIfError } from './common';

/*
OffsetForLeaderEpoch Request (Version: 2) => { [topics] }
  topics => { topic [partitions] }
    topic => STRING
    partitions => { partition current_leader_epoch leader_epoch }
      partition => INT32
      current_leader_epoch => INT32
      leader_epoch => INT32

OffsetForLeaderEpoch Response (Version: 2) => { throttle_time_ms [topics] }
  throttle_time_ms => INT32
  topics => { topic [partitions] }
    topic => STRING
    partitions => { error_code partition leader_epoch end_offset }
      error_code => INT16
      partition => INT32
      leader_epoch => INT32
      end_offset => INT64
*/
export const OFFSET_FOR_LEADER_EPOCH_V2 = createApi<OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse>({
    apiKey: 23,
    apiVersion: 2,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder.writeArray(data.topics, (encoder, topic) =>
            encoder
                .writeString(topic.topic)
                .writeArray(topic.partitions, (encoder, partition) =>
                    encoder
                        .writeInt32(partition.partition)
                        .writeInt32(partition.currentLeaderEpoch)
                        .writeInt32(partition.leaderEpoch),
                ),
        ),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readArray((topic) => ({
                topic: topic.readString()!,
                partitions: topic.readArray((partition) => ({
                    errorCode: partition.readInt16(),
                    partition: partition.readInt32(),
                    leaderEpoch: partition.readInt32(),
                    endOffset: partition.readInt64(),
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        }),
});
