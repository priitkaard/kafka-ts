import { createApi } from '../../utils/api';
import { AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, throwIfError } from './common';

/*
AddPartitionsToTxn Request (Version: 0) => { v3_and_below_transactional_id v3_and_below_producer_id v3_and_below_producer_epoch [v3_and_below_topics] }
  v3_and_below_transactional_id => STRING
  v3_and_below_producer_id => INT64
  v3_and_below_producer_epoch => INT16
  v3_and_below_topics => { name [partitions] }
    name => STRING
    partitions => INT32

AddPartitionsToTxn Response (Version: 0) => { throttle_time_ms [results_by_topic_v3_and_below] }
  throttle_time_ms => INT32
  results_by_topic_v3_and_below => { name [results_by_partition] }
    name => STRING
    results_by_partition => { partition_index partition_error_code }
      partition_index => INT32
      partition_error_code => INT16
*/
export const ADD_PARTITIONS_TO_TXN_V0 = createApi<AddPartitionsToTxnRequest, AddPartitionsToTxnResponse>({
    apiKey: 24,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeString(data.v3AndBelowTransactionalId ?? '')
            .writeInt64(data.v3AndBelowProducerId ?? 0n)
            .writeInt16(data.v3AndBelowProducerEpoch ?? 0)
            .writeArray(data.v3AndBelowTopics ?? [], (encoder, v3AndBelowTopic) =>
                encoder
                    .writeString(v3AndBelowTopic.name)
                    .writeArray(v3AndBelowTopic.partitions, (encoder, partition) => encoder.writeInt32(partition)),
            ),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            resultsByTopicV3AndBelow: decoder.readArray((resultsByTopicV3AndBelow) => ({
                name: resultsByTopicV3AndBelow.readString()!,
                resultsByPartition: resultsByTopicV3AndBelow.readArray((resultsByPartition) => ({
                    partitionIndex: resultsByPartition.readInt32(),
                    partitionErrorCode: resultsByPartition.readInt16(),
                    tags: {},
                })),
                tags: {},
            })),
            errorCode: 0,
            resultsByTransaction: [],
            tags: {},
        }),
});
