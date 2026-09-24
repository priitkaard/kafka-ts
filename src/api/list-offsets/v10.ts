import { createApi } from '../../utils/api';
import { LIST_OFFSETS_V9 } from './v9';

/*
ListOffsets Request (Version: 10) => { replica_id isolation_level (topics) timeout_ms }
  replica_id => INT32
  isolation_level => INT8
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index current_leader_epoch timestamp }
      partition_index => INT32
      current_leader_epoch => INT32
      timestamp => INT64
  timeout_ms => INT32

ListOffsets Response (Version: 10) => { throttle_time_ms (topics) }
  throttle_time_ms => INT32
  topics => { name (partitions) }
    name => COMPACT_STRING
    partitions => { partition_index error_code timestamp offset leader_epoch }
      partition_index => INT32
      error_code => INT16
      timestamp => INT64
      offset => INT64
      leader_epoch => INT32
*/
export const LIST_OFFSETS_V10 = createApi({
    ...LIST_OFFSETS_V9,
    apiVersion: 10,
    fallback: LIST_OFFSETS_V9,
    request: (encoder, data) =>
        encoder
            .writeInt32(data.replicaId)
            .writeInt8(data.isolationLevel)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeCompactString(topic.name)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partitionIndex)
                            .writeInt32(partition.currentLeaderEpoch)
                            .writeInt64(partition.timestamp)
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeInt32(data.timeoutMs ?? 30_000)
            .writeTagBuffer(),
});
