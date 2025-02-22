import { createApi } from '../utils/api';
import { Decoder } from '../utils/decoder';
import { KafkaTSApiError, KafkaTSError } from '../utils/error';
import { log } from '../utils/logger';

export const enum IsolationLevel {
    READ_UNCOMMITTED = 0,
    READ_COMMITTED = 1,
}

export type FetchResponse = Awaited<ReturnType<(typeof FETCH)['response']>>;

export const FETCH = createApi({
    apiKey: 1,
    apiVersion: 15,
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
    response: async (decoder) => {
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
                    records: decodeRecordBatch(partition),
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

const decodeRecordBatch = (decoder: Decoder) => {
    const size = decoder.readUVarInt() - 1;
    if (size <= 0) {
        return [];
    }

    const recordBatchDecoder = new Decoder(decoder.read(size));

    const results = [];
    while (recordBatchDecoder.canReadBytes(12)) {
        const baseOffset = recordBatchDecoder.readInt64();
        const batchLength = recordBatchDecoder.readInt32();
        if (!batchLength) {
            continue;
        }

        if (!recordBatchDecoder.canReadBytes(batchLength)) {
            // running into maxBytes limit
            recordBatchDecoder.read();
            continue;
        }

        const batchDecoder = new Decoder(recordBatchDecoder.read(batchLength));
        const partitionLeaderEpoch = batchDecoder.readInt32();
        const magic = batchDecoder.readInt8();
        if (magic !== 2) {
            throw new KafkaTSError(`Unsupported magic byte: ${magic}`);
        }

        const crc = batchDecoder.readInt32();
        const attributes = batchDecoder.readInt16();

        const compression = attributes & 0x07;
        const timestampType = (attributes & 0x08) >> 3 ? 'LogAppendTime' : 'CreateTime';
        const isTransactional = !!((attributes & 0x10) >> 4);
        const isControlBatch = !!((attributes & 0x20) >> 5);
        const hasDeleteHorizonMs = !!((attributes & 0x40) >> 6);

        if (compression !== 0) {
            throw new KafkaTSError(`Unsupported compression: ${compression}`);
        }

        const lastOffsetDelta = batchDecoder.readInt32();
        const baseTimestamp = batchDecoder.readInt64();
        const maxTimestamp = batchDecoder.readInt64();
        const producerId = batchDecoder.readInt64();
        const producerEpoch = batchDecoder.readInt16();
        const baseSequence = batchDecoder.readInt32();
        const records = decodeRecords(batchDecoder);

        results.push({
            baseOffset,
            batchLength,
            partitionLeaderEpoch,
            magic,
            crc,
            attributes,
            compression,
            timestampType,
            isTransactional,
            isControlBatch,
            hasDeleteHorizonMs,
            lastOffsetDelta,
            baseTimestamp,
            maxTimestamp,
            producerId,
            producerEpoch,
            baseSequence,
            records,
        });
    }
    return results;
};

const decodeRecords = (decoder: Decoder) =>
    decoder.readRecords((record) => ({
        attributes: record.readInt8(),
        timestampDelta: record.readVarLong(),
        offsetDelta: record.readVarInt(),
        key: record.readVarIntString(),
        value: record.readVarIntString(),
        headers: record.readVarIntArray((header) => ({
            key: header.readVarIntString()!,
            value: header.readVarIntString()!,
        })),
    }));
