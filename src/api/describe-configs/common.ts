import { KafkaTSApiError } from '../../utils/error';

export type DescribeConfigsRequest = {
    resources: {
        resourceType: number;
        resourceName: string;
        configurationKeys: string[] | null;
    }[];
    includeSynonyms: boolean;
    includeDocumentation?: boolean;
};

export type DescribeConfigsResponse = {
    throttleTimeMs: number;
    results: {
        errorCode: number;
        errorMessage: string | null;
        resourceType: number;
        resourceName: string;
        configs: {
            name: string;
            value: string | null;
            readOnly: boolean;
            configSource: number;
            isSensitive: boolean;
            synonyms: {
                name: string;
                value: string | null;
                source: number;
                tags: Record<number, Buffer>;
            }[];
            configType: number;
            documentation: string | null;
            tags: Record<number, Buffer>;
        }[];
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const throwIfError = <T extends DescribeConfigsResponse>(result: T) => {
    result.results.forEach((resultItem) => {
        if (resultItem.errorCode) throw new KafkaTSApiError(resultItem.errorCode, resultItem.errorMessage, result);
    });
    return result;
};
