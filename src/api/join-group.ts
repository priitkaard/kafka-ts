import { createApi } from "../utils/api";
import { Encoder } from "../utils/encoder";
import { KafkaTSApiError } from "../utils/error";

export const JOIN_GROUP = createApi({
    apiKey: 11,
    apiVersion: 9,
    request: (
        encoder,
        data: {
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
        },
    ) =>
        encoder
            .writeUVarInt(0)
            .writeCompactString(data.groupId)
            .writeInt32(data.sessionTimeoutMs)
            .writeInt32(data.rebalanceTimeoutMs)
            .writeCompactString(data.memberId)
            .writeCompactString(data.groupInstanceId)
            .writeCompactString(data.protocolType)
            .writeCompactArray(data.protocols, (encoder, protocol) => {
                const metadata = new Encoder()
                    .writeInt16(protocol.metadata.version)
                    .writeArray(protocol.metadata.topics, (encoder, topic) => encoder.writeString(topic))
                    .writeBytes(Buffer.alloc(0))
                    .value();
                return encoder.writeCompactString(protocol.name).writeCompactBytes(metadata).writeUVarInt(0);
            })
            .writeCompactString(data.reason)
            .writeUVarInt(0),
    response: (decoder) => {
        const result = {
            _tag: decoder.readTagBuffer(),
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            generationId: decoder.readInt32(),
            protocolType: decoder.readCompactString(),
            protocolName: decoder.readCompactString(),
            leader: decoder.readCompactString()!,
            skipAssignment: decoder.readBoolean(),
            memberId: decoder.readCompactString()!,
            members: decoder.readCompactArray((decoder) => ({
                memberId: decoder.readCompactString()!,
                groupInstanceId: decoder.readCompactString(),
                metadata: decoder.readCompactBytes()!,
                _tag: decoder.readTagBuffer(),
            })),
            _tag2: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        return result;
    },
});
