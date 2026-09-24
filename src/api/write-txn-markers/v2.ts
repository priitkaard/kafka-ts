import { createApi } from '../../utils/api';
import { WRITE_TXN_MARKERS_V1 } from './v1';

/*
WriteTxnMarkers Request (Version: 2) => { (markers) }
  markers => { producer_id producer_epoch transaction_result (topics) coordinator_epoch transaction_version }
    producer_id => INT64
    producer_epoch => INT16
    transaction_result => BOOLEAN
    topics => { name (partition_indexes) }
      name => COMPACT_STRING
      partition_indexes => INT32
    coordinator_epoch => INT32
    transaction_version => INT8

WriteTxnMarkers Response (Version: 2) => { (markers) }
  markers => { producer_id (topics) }
    producer_id => INT64
    topics => { name (partitions) }
      name => COMPACT_STRING
      partitions => { partition_index error_code }
        partition_index => INT32
        error_code => INT16
*/
export const WRITE_TXN_MARKERS_V2 = createApi({
    ...WRITE_TXN_MARKERS_V1,
    apiVersion: 2,
    fallback: WRITE_TXN_MARKERS_V1,
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
                    .writeInt8(marker.transactionVersion ?? 0)
                    .writeTagBuffer(),
            )
            .writeTagBuffer(),
});
