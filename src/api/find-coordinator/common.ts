import { KafkaTSApiError } from '../../utils/error';

export const KEY_TYPE = {
    GROUP: 0,
    TRANSACTION: 1,
};

export type FindCoordinatorRequest = {
    keyType: number;
    keys: string[];
};

export type FindCoordinatorResponse = {
    throttleTimeMs: number;
    coordinators: {
        key: string;
        nodeId: number;
        host: string;
        port: number;
        errorCode: number;
        errorMessage: string | null;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = (result: FindCoordinatorResponse) => {
    result.coordinators.forEach((coordinator) => {
        if (coordinator.errorCode) throw new KafkaTSApiError(coordinator.errorCode, coordinator.errorMessage, result);
    });
    return result;
};
