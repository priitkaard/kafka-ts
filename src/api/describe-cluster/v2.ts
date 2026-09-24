import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_CLUSTER_V1 } from './v1';

/*
DescribeCluster Request (Version: 2) => { include_cluster_authorized_operations endpoint_type include_fenced_brokers }
  include_cluster_authorized_operations => BOOLEAN
  endpoint_type => INT8
  include_fenced_brokers => BOOLEAN

DescribeCluster Response (Version: 2) => { throttle_time_ms error_code error_message endpoint_type cluster_id controller_id (brokers) cluster_authorized_operations }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  endpoint_type => INT8
  cluster_id => COMPACT_STRING
  controller_id => INT32
  brokers => { broker_id host port rack is_fenced }
    broker_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
    is_fenced => BOOLEAN
  cluster_authorized_operations => INT32
*/
export const DESCRIBE_CLUSTER_V2 = createApi({
    ...DESCRIBE_CLUSTER_V1,
    apiVersion: 2,
    fallback: DESCRIBE_CLUSTER_V1,
    request: (encoder, data) =>
        encoder
            .writeBoolean(data.includeClusterAuthorizedOperations)
            .writeInt8(data.endpointType ?? 1)
            .writeBoolean(data.includeFencedBrokers ?? false)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            endpointType: decoder.readInt8(),
            clusterId: decoder.readCompactString()!,
            controllerId: decoder.readInt32(),
            brokers: decoder.readCompactArray((broker) => ({
                brokerId: broker.readInt32(),
                host: broker.readCompactString()!,
                port: broker.readInt32(),
                rack: broker.readCompactString(),
                isFenced: broker.readBoolean(),
                tags: broker.readTagBuffer(),
            })),
            clusterAuthorizedOperations: decoder.readInt32(),
            tags: decoder.readTagBuffer(),
        }),
});
