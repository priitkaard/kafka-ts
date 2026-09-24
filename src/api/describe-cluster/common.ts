import { KafkaTSApiError } from '../../utils/error';

export type DescribeClusterRequest = {
    includeClusterAuthorizedOperations: boolean;
    endpointType?: number;
    includeFencedBrokers?: boolean;
};

export type DescribeClusterResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    endpointType: number;
    clusterId: string;
    controllerId: number;
    brokers: {
        brokerId: number;
        host: string;
        port: number;
        rack: string | null;
        isFenced: boolean;
        tags: Record<number, Buffer>;
    }[];
    clusterAuthorizedOperations: number;
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DescribeClusterResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    return result;
};
