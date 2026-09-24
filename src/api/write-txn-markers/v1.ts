import { createApi } from '../../utils/api';
import { throwIfError, WriteTxnMarkersRequest, WriteTxnMarkersResponse } from './common';

/*
WriteTxnMarkers Request (Version: 1) => { (markers) }
  markers => { producer_id producer_epoch transaction_result (topics) coordinator_epoch }
    producer_id => INT64
    producer_epoch => INT16
    transaction_result => BOOLEAN
    topics => { name (partition_indexes) }
      name => COMPACT_STRING
      partition_indexes => INT32
    coordinator_epoch => INT32

WriteTxnMarkers Response (Version: 1) => { (markers) }
  markers => { producer_id (topics) }
    producer_id => INT64
    topics => { name (partitions) }
      name => COMPACT_STRING
      partitions => { partition_index error_code }
        partition_index => INT32
        error_code => INT16
*/
export const WRITE_TXN_MARKERS_V1 = createApi<WriteTxnMarkersRequest, WriteTxnMarkersResponse>({
    apiKey: 27,
    apiVersion: 1,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.markers, (encoder, marker) =>
                encoder
                    .writeInt64(marker.producerId)
                    .writeInt16(marker.producerEpoch)
                    .writeBoolean(marker.transactionResult)
                    .writeCompactArray(marker.topics, (encoder, topic) =>
                        encoder
                            .writeCompactString(topic.name)
                            .writeCompactArray(topic.partitionIndexes, (encoder, partitionIndex) =>
                                encoder.writeInt32(partitionIndex),
                            )
                            .writeTagBuffer(),
                    )
                    .writeInt32(marker.coordinatorEpoch)
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            markers: decoder.readCompactArray((marker) => ({
                producerId: marker.readInt64(),
                topics: marker.readCompactArray((topic) => ({
                    name: topic.readCompactString()!,
                    partitions: topic.readCompactArray((partition) => ({
                        partitionIndex: partition.readInt32(),
                        errorCode: partition.readInt16(),
                        tags: partition.readTagBuffer(),
                    })),
                    tags: topic.readTagBuffer(),
                })),
                tags: marker.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
