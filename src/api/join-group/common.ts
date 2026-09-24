import { Encoder } from '../../utils/encoder';
import { KafkaTSApiError } from '../../utils/error';

export type JoinGroupRequest = {
    groupId: string;
    sessionTimeoutMs: number;
    rebalanceTimeoutMs: number;
    memberId: string;
    groupInstanceId: string | null;
    protocolType: string;
    protocols: {
        name: string;
        metadata: {
            version: number;
            topics: string[];
        };
    }[];
    reason: string | null;
};

export type JoinGroupResponse = {
    throttleTimeMs: number;
    errorCode: number;
    generationId: number;
    protocolType: string | null;
    protocolName: string | null;
    leader: string;
    skipAssignment: boolean;
    memberId: string;
    members: {
        memberId: string;
        groupInstanceId: string | null;
        metadata: Buffer;
        tags: Record<number, Buffer>;
    }[];
    tags: Record<number, Buffer>;
};

export const encodeProtocolMetadata = ({ version, topics }: JoinGroupRequest['protocols'][number]['metadata']) =>
    new Encoder()
        .writeInt16(version)
        .writeArray(topics, (encoder, topic) => encoder.writeString(topic))
        .writeBytes(Buffer.alloc(0))
        .value();

export const throwIfError = (result: JoinGroupResponse) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    return result;
};
