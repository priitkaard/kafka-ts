import { KafkaTSApiError } from '../../utils/error';

export type UpdateFeaturesRequest = {
    timeoutMs: number;
    featureUpdates: {
        feature: string;
        maxVersionLevel: number;
        allowDowngrade?: boolean;
        upgradeType?: number;
    }[];
    validateOnly?: boolean;
};

export type UpdateFeaturesResponse = {
    throttleTimeMs: number;
    errorCode: number;
    errorMessage: string | null;
    results: {
        feature: string;
        errorCode: number;
        errorMessage: string | null;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends UpdateFeaturesResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, result.errorMessage, result);
    result.results.forEach((resultItem) => {
        if (resultItem.errorCode) throw new KafkaTSApiError(resultItem.errorCode, resultItem.errorMessage, result);
    });
    return result;
};
