import { createApi } from '../utils/api';
import { Decoder } from '../utils/decoder';
import { KafkaTSApiError, KafkaTSError } from '../utils/error';

export const enum IsolationLevel {
    READ_UNCOMMITTED = 0,
    READ_COMMITTED = 1,
}

type FetchRequest = {
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

/*
Fetch Request (Version: 4) => replica_id max_wait_ms min_bytes max_bytes isolation_level [topics] 
  replica_id => INT32
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  topics => topic [partitions] 
    topic => STRING
    partitions => partition fetch_offset partition_max_bytes 
      partition => INT32
      fetch_offset => INT64
      partition_max_bytes => INT32

Fetch Response (Version: 4) => throttle_time_ms [responses] 
  throttle_time_ms => INT32
  responses => topic [partitions] 
    topic => STRING
    partitions => partition_index error_code high_watermark last_stable_offset [aborted_transactions] records 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      aborted_transactions => producer_id first_offset 
        producer_id => INT64
        first_offset => INT64
      records => RECORDS
*/
const FETCH_V4 = createApi<FetchRequest, FetchResponse>({
    apiKey: 1,
    apiVersion: 4,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeInt32(-1) // replica_id
            .writeInt32(data.maxWaitMs)
            .writeInt32(data.minBytes)
            .writeInt32(data.maxBytes)
            .writeInt8(data.isolationLevel)
            .writeArray(data.topics, (encoder, topic) =>
                encoder
                    .writeString(topic.topicName)
                    .writeArray(topic.partitions, (encoder, partition) =>
                        encoder
                            .writeInt32(partition.partition)
                            .writeInt64(partition.fetchOffset)
                            .writeInt32(partition.partitionMaxBytes),
                    ),
            ),
    response: (decoder) => {
        const result = {
            throttleTimeMs: decoder.readInt32(),
            errorCode: 0,
            sessionId: 0,
            responses: decoder.readArray((response) => ({
                topicName: response.readString()!,
                partitions: response.readArray((partition) => ({
                    partitionIndex: partition.readInt32(),
                    errorCode: partition.readInt16(),
                    highWatermark: partition.readInt64(),
                    lastStableOffset: partition.readInt64(),
                    logStartOffset: BigInt(0), // Not present in v4 response
                    abortedTransactions: partition.readArray((abortedTransaction) => ({
                        producerId: abortedTransaction.readInt64(),
                        firstOffset: abortedTransaction.readInt64(),
                    })),
                    preferredReadReplica: -1, // Not present in v4 response
                    records: decodeRecordBatch(partition, partition.readInt32()),
                })),
            })),
        };
        result.responses.forEach((response) => {
            response.partitions.forEach((partition) => {
                if (partition.errorCode) throw new KafkaTSApiError(partition.errorCode, null, result);
            });
        });
        return result;
    },
});

/*
Fetch Request (Version: 15) => max_wait_ms min_bytes max_bytes isolation_level session_id session_epoch [topics] [forgotten_topics_data] rack_id _tagged_fields 
  max_wait_ms => INT32
  min_bytes => INT32
  max_bytes => INT32
  isolation_level => INT8
  session_id => INT32
  session_epoch => INT32
  topics => topic_id [partitions] _tagged_fields 
    topic_id => UUID
    partitions => partition current_leader_epoch fetch_offset last_fetched_epoch log_start_offset partition_max_bytes _tagged_fields 
      partition => INT32
      current_leader_epoch => INT32
      fetch_offset => INT64
      last_fetched_epoch => INT32
      log_start_offset => INT64
      partition_max_bytes => INT32
  forgotten_topics_data => topic_id [partitions] _tagged_fields 
    topic_id => UUID
    partitions => INT32
  rack_id => COMPACT_STRING

Fetch Response (Version: 15) => throttle_time_ms error_code session_id [responses] _tagged_fields 
  throttle_time_ms => INT32
  error_code => INT16
  session_id => INT32
  responses => topic_id [partitions] _tagged_fields 
    topic_id => UUID
    partitions => partition_index error_code high_watermark last_stable_offset log_start_offset [aborted_transactions] preferred_read_replica records _tagged_fields 
      partition_index => INT32
      error_code => INT16
      high_watermark => INT64
      last_stable_offset => INT64
      log_start_offset => INT64
      aborted_transactions => producer_id first_offset _tagged_fields 
        producer_id => INT64
        first_offset => INT64
      preferred_read_replica => INT32
      records => COMPACT_RECORDS
*/
export const FETCH = createApi<FetchRequest, FetchResponse>({
    apiKey: 1,
    apiVersion: 15,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    fallback: FETCH_V4,
    request: (encoder, data) =>
        encoder
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
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeCompactArray(data.forgottenTopicsData, (encoder, forgottenTopic) =>
                encoder
                    .writeUUID(forgottenTopic.topicId)
                    .writeCompactArray(forgottenTopic.partitions, (encoder, partition) => encoder.writeInt32(partition))
                    .writeTagBuffer(),
            )
            .writeCompactString(data.rackId)
            .writeTagBuffer(),
    response: async (decoder) => {
        const result = {
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
                        tags: abortedTransaction.readTagBuffer(),
                    })),
                    preferredReadReplica: partition.readInt32(),
                    records: decodeRecordBatch(partition, partition.readUVarInt() - 1),
                    tags: partition.readTagBuffer(),
                })),
                tags: response.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
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

const decodeRecordBatch = (decoder: Decoder, size: number) => {
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

        if (compression !== 0) {
            throw new KafkaTSError(`Unsupported compression: ${compression}`);
        }

        const lastOffsetDelta = batchDecoder.readInt32();
        const baseTimestamp = batchDecoder.readInt64();
        const maxTimestamp = batchDecoder.readInt64();
        const deleteHorizonMs = hasDeleteHorizonMs ? baseTimestamp : null;
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
            deleteHorizonMs,
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
