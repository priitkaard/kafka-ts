import { createApi } from '../../utils/api';
import { DescribeProducersRequest, DescribeProducersResponse, throwIfError } from './common';

/*
DescribeProducers Request (Version: 0) => { (topics) }
  topics => { name (partition_indexes) }
    name => COMPACT_STRING
    partition_indexes => INT32

DescribeProducers Response (Version: 0) => { throttle_time_ms (topics) }
  throttle_time_ms => INT32
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index error_code error_message (active_producers) }
      partition_index => INT32
      error_code => INT16
      error_message => COMPACT_NULLABLE_STRING
      active_producers => { producer_id producer_epoch last_sequence last_timestamp coordinator_epoch current_txn_start_offset }
        producer_id => INT64
        producer_epoch => INT32
        last_sequence => INT32
        last_timestamp => INT64
        coordinator_epoch => INT32
        current_txn_start_offset => INT64
*/
export const DESCRIBE_PRODUCERS_V0 = createApi<DescribeProducersRequest, DescribeProducersResponse>({
    apiKey: 61,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitionIndexes, (encoder, partitionIndex) =>
                        encoder.writeInt32(partitionIndex),
                    )
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            topics: decoder.readCompactArray((topic) => ({
                name: topic.readCompactString()!,
                partitions: topic.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    errorMessage: partition.readCompactString(),
                    activeProducers: partition.readCompactArray((activeProducer) => ({
                        producerId: activeProducer.readInt64(),
                        producerEpoch: activeProducer.readInt32(),
                        lastSequence: activeProducer.readInt32(),
                        lastTimestamp: activeProducer.readInt64(),
                        coordinatorEpoch: activeProducer.readInt32(),
                        currentTxnStartOffset: activeProducer.readInt64(),
                        tags: activeProducer.readTagBuffer(),
                    })),
                    tags: partition.readTagBuffer(),
                })),
                tags: topic.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
