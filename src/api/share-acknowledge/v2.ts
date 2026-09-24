import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { SHARE_ACKNOWLEDGE_V1 } from './v1';

/*
ShareAcknowledge Request (Version: 2) => { group_id member_id share_session_epoch is_renew_ack (topics) }
  group_id => COMPACT_NULLABLE_STRING
  member_id => COMPACT_NULLABLE_STRING
  share_session_epoch => INT32
  is_renew_ack => BOOLEAN
  topics => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition_index (acknowledgement_batches) }
      partition_index => INT32
      acknowledgement_batches => { first_offset last_offset (acknowledge_types) }
        first_offset => INT64
        last_offset => INT64
        acknowledge_types => INT8

ShareAcknowledge Response (Version: 2) => { throttle_time_ms error_code error_message acquisition_lock_timeout_ms (responses) (node_endpoints) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  acquisition_lock_timeout_ms => INT32
  responses => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition_index error_code error_message current_leader }
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      current_leader => { leader_id leader_epoch }
        leader_id => INT32
        leader_epoch => INT32
  node_endpoints => { node_id host port rack }
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
*/
export const SHARE_ACKNOWLEDGE_V2 = createApi({
    ...SHARE_ACKNOWLEDGE_V1,
    apiVersion: 2,
    fallback: SHARE_ACKNOWLEDGE_V1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeCompactString(data.memberId)
            .writeInt32(data.shareSessionEpoch)
            .writeBoolean(data.isRenewAck ?? false)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeUUID(topic.topicId)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeCompactArray(partition.acknowledgementBatches, (encoder, acknowledgementBatche) =>
                                encoder
                                    .writeInt64(acknowledgementBatche.firstOffset)
                                    .writeInt64(acknowledgementBatche.lastOffset)
                                    .writeCompactArray(
                                        acknowledgementBatche.acknowledgeTypes,
                                        (encoder, acknowledgeType) => encoder.writeInt8(acknowledgeType),
                                    )
                                    .writeTagBuffer(),
                            )
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            acquisitionLockTimeoutMs: decoder.readInt32(),
            responses: decoder.readCompactArray((response) => ({
                topicId: response.readUUID(),
                partitions: response.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    errorMessage: partition.readCompactString(),
                    currentLeader: partition.readStruct((currentLeader) => ({
                        leaderId: currentLeader.readInt32(),
                        leaderEpoch: currentLeader.readInt32(),
                        tags: currentLeader.readTagBuffer(),
                    })),
                    tags: partition.readTagBuffer(),
                })),
                tags: response.readTagBuffer(),
            })),
            nodeEndpoints: decoder.readCompactArray((nodeEndpoint) => ({
                nodeId: nodeEndpoint.readInt32(),
                host: nodeEndpoint.readCompactString()!,
                port: nodeEndpoint.readInt32(),
                rack: nodeEndpoint.readCompactString(),
                tags: nodeEndpoint.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
