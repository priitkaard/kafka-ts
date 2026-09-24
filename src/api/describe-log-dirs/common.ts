import { KafkaTSApiError } from '../../utils/error';

export type DescribeLogDirsRequest = {
    topics:
        | {
              topic: string;
              partitions: number[];
          }[]
        | null;
};

export type DescribeLogDirsResponse = {
    throttleTimeMs: number;
    errorCode: number;
    results: {
        errorCode: number;
        logDir: string;
        topics: {
            name: string;
            partitions: {
                partitionIndex: number;
                partitionSize: bigint;
                offsetLag: bigint;
                isFutureKey: boolean;
                tags: Record<number, Buffer>;
            }[];
            tags: Record<number, Buffer>;
        }[];
        totalBytes: bigint;
        usableBytes: bigint;
        isCordoned: boolean;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DescribeLogDirsResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    result.results.forEach((resultItem) => {
        if (resultItem.errorCode) throw new KafkaTSApiError(resultItem.errorCode, null, result);
    });
    return result;
};
