import { KafkaTSApiError } from '../../utils/error';

export type AlterClientQuotasRequest = {
    entries: {
        entity: {
            entityType: string;
            entityName: string | null;
        }[];
        ops: {
            key: string;
            value: number;
            remove: boolean;
        }[];
    }[];
    validateOnly: boolean;
};

export type AlterClientQuotasResponse = {
    throttleTimeMs: number;
    entries: {
        errorCode: number;
        errorMessage: string | null;
        entity: {
            entityType: string;
            entityName: string | null;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends AlterClientQuotasResponse>(result: T) => {
    result.entries.forEach((entry) => {
        if (entry.errorCode) throw new KafkaTSApiError(entry.errorCode, entry.errorMessage, result);
    });
    return result;
};
