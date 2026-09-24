import { createApi } from '../../utils/api';
import { DescribeClusterRequest, DescribeClusterResponse, throwIfError } from './common';

/*
DescribeCluster Request (Version: 0) => { include_cluster_authorized_operations }
  include_cluster_authorized_operations => BOOLEAN

DescribeCluster Response (Version: 0) => { throttle_time_ms error_code error_message cluster_id controller_id (brokers) cluster_authorized_operations }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  cluster_id => COMPACT_STRING
  controller_id => INT32
  brokers => { broker_id host port rack }
    broker_id => INT32
    host => COMPACT_STRING
    port => INT32
    rack => COMPACT_NULLABLE_STRING
  cluster_authorized_operations => INT32
*/
export const DESCRIBE_CLUSTER_V0 = createApi<DescribeClusterRequest, DescribeClusterResponse>({
    apiKey: 60,
    apiVersion: 0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) => encoder.writeBoolean(data.includeClusterAuthorizedOperations).writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            endpointType: 1,
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
