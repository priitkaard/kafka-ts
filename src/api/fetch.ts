import { createApi } from '../utils/api';
import { Decoder } from '../utils/decoder';
import { KafkaTSApiError } from '../utils/error';

export const enum IsolationLevel {
    READ_UNCOMMITTED = 0,
    READ_COMMITTED = 1,
}

export const FETCH = createApi({
    apiKey: 1,
    apiVersion: 16,
    request: (
        encoder,
        data: {
            maxWaitMs: number;
            minBytes: number;
            maxBytes: number;
            isolationLevel: IsolationLevel;
            sessionId: number;
            sessionEpoch: number;
            topics: {
                topicId: string;
                partitions: {
                    partition: number;
                    currentLeaderEpoch: number;
                    fetchOffset: bigint;
                    lastFetchedEpoch: number;
                    logStartOffset: bigint;
                    partitionMaxBytes: number;
                }[];
            }[];
            forgottenTopicsData: {
                topicId: string;
                partitions: number[];
            }[];
            rackId: string;
        },
    ) =>
        encoder
            .writeUVarInt(0)
            .writeInt32(data.maxWaitMs)
            .writeInt32(data.minBytes)
            .writeInt32(data.maxBytes)
            .writeInt8(data.isolationLevel)
            .writeInt32(data.sessionId)
            .writeInt32(data.sessionEpoch)
            .writeCompactArray(data.topics, (encoder, topic) =>
                encoder
                    .writeUUID(topic.topicId)
                    .writeCompactArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partition)
                            .writeInt32(partition.currentLeaderEpoch)
                            .writeInt64(partition.fetchOffset)
                            .writeInt32(partition.lastFetchedEpoch)
                            .writeInt64(partition.logStartOffset)
                            .writeInt32(partition.partitionMaxBytes)
                            .writeUVarInt(0),
                    )
                    .writeUVarInt(0),
            )
            .writeCompactArray(data.forgottenTopicsData, (encoder, forgottenTopic) =>
                encoder
                    .writeUUID(forgottenTopic.topicId)
                    .writeCompactArray(forgottenTopic.partitions, (encoder, partition) => encoder.writeInt32(partition))
                    .writeUVarInt(0),
            )
            .writeCompactString(data.rackId)
            .writeUVarInt(0),
    response: (decoder) => {
        const result = {
            _tag: decoder.readTagBuffer(),
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            sessionId: decoder.readInt32(),
            responses: decoder.readCompactArray((response) => ({
                topicId: response.readUUID(),
                partitions: response.readCompactArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    highWatermark: partition.readInt64(),
                    lastStableOffset: partition.readInt64(),
                    logStartOffset: partition.readInt64(),
                    abortedTransactions: partition.readCompactArray((abortedTransaction) => ({
                        producerId: abortedTransaction.readInt64(),
                        firstOffset: abortedTransaction.readInt64(),
                        _tag: abortedTransaction.readTagBuffer(),
                    })),
                    preferredReadReplica: partition.readInt32(),
                    records: decodeRecords(partition),
                    _tag: partition.readTagBuffer(),
                })),
                _tag: response.readTagBuffer(),
            })),
            _tag2: decoder.readTagBuffer(),
        };
        if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
        result.responses.forEach((response) => {
            response.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
            });
        });
        return result;
    },
});

const decodeRecords = (decoder: Decoder) => {
    const size = decoder.readUVarInt() - 1;
    if (size <= 0) {
        return [];
    }

    const results = [];
    while (decoder.getBufferLength() > decoder.getOffset() + 49) {
        results.push({
            baseOffset: decoder.readInt64(),
            batchLength: decoder.readInt32(),
            partitionLeaderEpoch: decoder.readInt32(),
            magic: decoder.readInt8(),
            crc: decoder.readUInt32(),
            attributes: decoder.readInt16(),
            lastOffsetDelta: decoder.readInt32(),
            baseTimestamp: decoder.readInt64(),
            maxTimestamp: decoder.readInt64(),
            producerId: decoder.readInt64(),
            producerEpoch: decoder.readInt16(),
            baseSequence: decoder.readInt32(),
            records: decoder.readRecords((record) => ({
                attributes: record.readInt8(),
                timestampDelta: record.readVarLong(),
                offsetDelta: record.readVarInt(),
                key: record.readVarIntBuffer(),
                value: record.readVarIntBuffer(),
                headers: record.readCompactArray((header) => ({
                    key: header.readVarIntBuffer(),
                    value: header.readVarIntBuffer(),
                })),
            })),
        });
    }
    return results;
};
