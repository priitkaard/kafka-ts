import { createApi } from '../../utils/api';
import { ShareAcknowledgeRequest, ShareAcknowledgeResponse, throwIfError } from './common';

/*
ShareAcknowledge Request (Version: 1) => { group_id member_id share_session_epoch (topics) }
  group_id => COMPACT_NULLABLE_STRING
  member_id => COMPACT_NULLABLE_STRING
  share_session_epoch => INT32
  topics => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition_index (acknowledgement_batches) }
      partition_index => INT32
      acknowledgement_batches => { first_offset last_offset (acknowledge_types) }
        first_offset => INT64
        last_offset => INT64
        acknowledge_types => INT8

ShareAcknowledge Response (Version: 1) => { throttle_time_ms error_code error_message (responses) (node_endpoints) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
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
export const SHARE_ACKNOWLEDGE_V1 = createApi<ShareAcknowledgeRequest, ShareAcknowledgeResponse>({
    apiKey: 79,
    apiVersion: 1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeCompactString(data.memberId)
            .writeInt32(data.shareSessionEpoch)
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
            acquisitionLockTimeoutMs: 0,
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
