import { createApi } from '../../utils/api';
import { decodeRecordBatch } from '../fetch';
import { ShareFetchRequest, ShareFetchResponse, throwIfError } from './common';

/*
ShareFetch Request (Version: 1) => { group_id member_id share_session_epoch max_wait_ms min_bytes max_bytes max_records batch_size (topics) (forgotten_topics_data) }
  group_id => COMPACT_NULLABLE_STRING
  member_id => COMPACT_NULLABLE_STRING
  share_session_epoch => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  max_records => INT32
  batch_size => INT32
  topics => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition_index (acknowledgement_batches) }
      partition_index => INT32
      acknowledgement_batches => { first_offset last_offset (acknowledge_types) }
        first_offset => INT64
        last_offset => INT64
        acknowledge_types => INT8
  forgotten_topics_data => { topic_id (partitions) }
    topic_id => UUID
    partitions => INT32

ShareFetch Response (Version: 1) => { throttle_time_ms error_code error_message acquisition_lock_timeout_ms (responses) (node_endpoints) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  acquisition_lock_timeout_ms => INT32
  responses => { topic_id (partitions) }
    topic_id => UUID
    partitions => { partition_index error_code error_message acknowledge_error_code acknowledge_error_message current_leader records (acquired_records) }
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      acknowledge_error_code => INT16
      acknowledge_error_message => COMPACT_NULLABLE_STRING
      current_leader => { leader_id leader_epoch }
        leader_id => INT32
        leader_epoch => INT32
      records => COMPACT_RECORDS
      acquired_records => { first_offset last_offset delivery_count }
        first_offset => INT64
        last_offset => INT64
        delivery_count => INT16
  node_endpoints => { node_id host port rack }
    node_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
*/
export const SHARE_FETCH_V1 = createApi<ShareFetchRequest, ShareFetchResponse>({
    apiKey: 78,
    apiVersion: 1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.groupId)
            .writeCompactString(data.memberId)
            .writeInt32(data.shareSessionEpoch)
            .writeInt32(data.maxWaitMs)
            .writeInt32(data.minBytes)
            .writeInt32(data.maxBytes)
            .writeInt32(data.maxRecords)
            .writeInt32(data.batchSize)
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
            .writeCompactArray(data.forgottenTopicsData, (encoder, forgottenTopicsData) =>
                encoder
                    .writeUUID(forgottenTopicsData.topicId)
                    .writeCompactArray(forgottenTopicsData.partitions, (encoder, partition) =>
                        encoder.writeInt32(partition),
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
                    acknowledgeErrorCode: partition.readInt16(),
                    acknowledgeErrorMessage: partition.readCompactString(),
                    currentLeader: partition.readStruct((currentLeader) => ({
                        leaderId: currentLeader.readInt32(),
                        leaderEpoch: currentLeader.readInt32(),
                        tags: currentLeader.readTagBuffer(),
                    })),
                    records: decodeRecordBatch(partition, partition.readUVarInt() - 1),
                    acquiredRecords: partition.readCompactArray((acquiredRecord) => ({
                        firstOffset: acquiredRecord.readInt64(),
                        lastOffset: acquiredRecord.readInt64(),
                        deliveryCount: acquiredRecord.readInt16(),
                        tags: acquiredRecord.readTagBuffer(),
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
