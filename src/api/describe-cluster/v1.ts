import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_CLUSTER_V0 } from './v0';

/*
DescribeCluster Request (Version: 1) => { include_cluster_authorized_operations endpoint_type }
  include_cluster_authorized_operations => BOOLEAN
  endpoint_type => INT8

DescribeCluster Response (Version: 1) => { throttle_time_ms error_code error_message endpoint_type cluster_id controller_id (brokers) cluster_authorized_operations }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  endpoint_type => INT8
  cluster_id => COMPACT_STRING
  controller_id => INT32
  brokers => { broker_id host port rack }
    broker_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_authorized_operations => INT32
*/
export const DESCRIBE_CLUSTER_V1 = createApi({
    ...DESCRIBE_CLUSTER_V0,
    apiVersion: 1,
    fallback: DESCRIBE_CLUSTER_V0,
    request: (encoder, data) =>
        encoder
            .writeBoolean(data.includeClusterAuthorizedOperations)
            .writeInt8(data.endpointType ?? 1)
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
                isFenced: false,
                tags: broker.readTagBuffer(),
            })),
            clusterAuthorizedOperations: decoder.readInt32(),
            tags: decoder.readTagBuffer(),
        }),
});
