import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { ADD_PARTITIONS_TO_TXN_V2 } from './v2';

/*
AddPartitionsToTxn Request (Version: 3) => { v3_and_below_transactional_id v3_and_below_producer_id v3_and_below_producer_epoch (v3_and_below_topics) }
  v3_and_below_transactional_id => COMPACT_STRING
  v3_and_below_producer_id => INT64
  v3_and_below_producer_epoch => INT16
  v3_and_below_topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => INT32

AddPartitionsToTxn Response (Version: 3) => { throttle_time_ms (results_by_topic_v3_and_below) }
  throttle_time_ms => INT32
  results_by_topic_v3_and_below => { name (results_by_partition) }
    name => COMPACT_STRING
    results_by_partition => { partition_index partition_error_code }
      partition_index => INT32
      partition_error_code => INT16
*/
export const ADD_PARTITIONS_TO_TXN_V3 = createApi({
    ...ADD_PARTITIONS_TO_TXN_V2,
    apiVersion: 3,
    fallback: ADD_PARTITIONS_TO_TXN_V2,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactString(data.v3AndBelowTransactionalId ?? '')
            .writeInt64(data.v3AndBelowProducerId ?? 0n)
            .writeInt16(data.v3AndBelowProducerEpoch ?? 0)
            .writeCompactArray(data.v3AndBelowTopics ?? [], (encoder, v3AndBelowTopic) =>
                encoder
                    .writeCompactString(v3AndBelowTopic.name)
                    .writeCompactArray(v3AndBelowTopic.partitions, (encoder, partition) =>
                        encoder.writeInt32(partition),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            resultsByTopicV3AndBelow: decoder.readCompactArray((resultsByTopicV3AndBelow) => ({
                name: resultsByTopicV3AndBelow.readCompactString()!,
                resultsByPartition: resultsByTopicV3AndBelow.readCompactArray((resultsByPartition) => ({
                    partitionIndex: resultsByPartition.readInt32(),
                    partitionErrorCode: resultsByPartition.readInt16(),
                    tags: resultsByPartition.readTagBuffer(),
                })),
                tags: resultsByTopicV3AndBelow.readTagBuffer(),
            })),
            errorCode: 0,
            resultsByTransaction: [],
            tags: decoder.readTagBuffer(),
        }),
});
