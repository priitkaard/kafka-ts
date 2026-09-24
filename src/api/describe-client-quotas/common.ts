import { KafkaTSApiError } from '../../utils/error';

export type DescribeClientQuotasRequest = {
    components: {
        entityType: string;
        matchType: number;
        match: string | null;
    }[];
    strict: boolean;
};

export type DescribeClientQuotasResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    entries: {
        entity: {
            entityType: string;
            entityName: string | null;
            tags: Record<number, Buffer>;
        }[];
        values: {
            key: string;
            value: number;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DescribeClientQuotasResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    return result;
};
