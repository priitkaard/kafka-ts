import { findCodec } from '../../codecs';
import { Decoder } from '../../utils/decoder';
import { KafkaTSApiError, KafkaTSError } from '../../utils/error';

export const enum IsolationLevel {
    READ_UNCOMMITTED = 0,
    READ_COMMITTED = 1,
}

export type FetchRequest = {
    maxWaitMs: number;
    minBytes: number;
    maxBytes: number;
    isolationLevel: IsolationLevel;
    sessionId: number;
    sessionEpoch: number;
    topics: {
        topicId: string;
        topicName: string;
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
        topicName: string;
        partitions: number[];
    }[];
    rackId: string;
};

export type FetchResponse = {
    throttleTimeMs: number;
    errorCode: number;
    sessionId: number;
    responses: (({ topicId: string } | { topicName: string }) & {
        partitions: {
            partitionIndex: number;
            errorCode: number;
            highWatermark: bigint;
            lastStableOffset: bigint;
            logStartOffset: bigint;
            abortedTransactions: {
                producerId: bigint;
                firstOffset: bigint;
            }[];
            preferredReadReplica: number;
            records: {
                baseOffset: bigint;
                batchLength: number;
                partitionLeaderEpoch: number;
                magic: number;
                crc: number;
                attributes: number;
                compression: number;
                timestampType: 'CreateTime' | 'LogAppendTime';
                isTransactional: boolean;
                isControlBatch: boolean;
                hasDeleteHorizonMs: boolean;
                deleteHorizonMs: bigint | null;
                lastOffsetDelta: number;
                baseTimestamp: bigint;
                maxTimestamp: bigint;
                producerId: bigint;
                producerEpoch: number;
                baseSequence: number;
                records: {
                    attributes: number;
                    timestampDelta: bigint;
                    offsetDelta: number;
                    key: string | null;
                    value: string | null;
                    headers: {
                        key: string;
                        value: string;
                    }[];
                }[];
            }[];
        }[];
    })[];
};

export const throwIfError = <T extends FetchResponse>(result: T) => {
    if (result.errorCode) throw new KafkaTSApiError(result.errorCode, null, result);
    result.responses.forEach((response) => {
        response.partitions.forEach((partition) => {
            if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
        });
    });
    return result;
};

export const withDecompressions = async <T>(decode: (decompressions: Promise<void>[]) => T) => {
    const decompressions: Promise<void>[] = [];
    try {
        return decode(decompressions);
    } finally {
        await Promise.all(decompressions);
    }
};

export const decodeRecordBatch = (decoder: Decoder, size: number, decompressions: Promise<void>[]) => {
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
        const timestampType = (attributes & 0x08) >> 3 ? ('LogAppendTime' as const) : ('CreateTime' as const);
        const isTransactional = !!((attributes & 0x10) >> 4);
        const isControlBatch = !!((attributes & 0x20) >> 5);
        const hasDeleteHorizonMs = !!((attributes & 0x40) >> 6);

        const lastOffsetDelta = batchDecoder.readInt32();
        const baseTimestamp = batchDecoder.readInt64();
        const maxTimestamp = batchDecoder.readInt64();
        const deleteHorizonMs = hasDeleteHorizonMs ? baseTimestamp : null;
        const producerId = batchDecoder.readInt64();
        const producerEpoch = batchDecoder.readInt16();
        const baseSequence = batchDecoder.readInt32();
        const recordsCount = batchDecoder.readInt32();

        const batch = {
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
            deleteHorizonMs,
            lastOffsetDelta,
            baseTimestamp,
            maxTimestamp,
            producerId,
            producerEpoch,
            baseSequence,
            records: compression ? [] : decodeRecords(batchDecoder, recordsCount),
        };
        if (compression) {
            const payload = batchDecoder.read();
            decompressions.push(
                findCodec(compression)
                    .decompress(payload)
                    .then((data) => {
                        batch.records = decodeRecords(new Decoder(data), recordsCount);
                    }),
            );
        }
        results.push(batch);
    }
    return results;
};

const decodeRecords = (decoder: Decoder, length: number) =>
    decoder.readRecords(length, (record) => ({
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
