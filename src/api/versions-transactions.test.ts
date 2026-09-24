import { describe, expect } from 'vitest';
import { API } from '.';
import { ADD_PARTITIONS_TO_TXN_V3 } from './add-partitions-to-txn/v3';
import { KEY_TYPE } from './find-coordinator';
import { createPartitionData, createVersionTestContext, randomName } from './versions.test-utils';

describe.sequential('Transaction API versions', () => {
    const context = createVersionTestContext('transactions');
    const { topicName, forEachVersion, findCoordinator } = context;

    const beginTransaction = async (prefix: string) => {
        const transactionalId = randomName(prefix);
        const sendRequest = await findCoordinator(transactionalId, KEY_TYPE.TRANSACTION);
        const { producerId, producerEpoch } = await sendRequest(API.INIT_PRODUCER_ID, {
            transactionalId,
            transactionTimeoutMs: 60_000,
            producerId: -1n,
            producerEpoch: -1,
        });
        return { transactionalId, sendRequest, producerId, producerEpoch };
    };

    const beginTransactionWithPartition = async (prefix: string) => {
        const transaction = await beginTransaction(prefix);
        const { transactionalId, producerId, producerEpoch, sendRequest } = transaction;
        await sendRequest(ADD_PARTITIONS_TO_TXN_V3, {
            v3AndBelowTransactionalId: transactionalId,
            v3AndBelowProducerId: producerId,
            v3AndBelowProducerEpoch: producerEpoch,
            v3AndBelowTopics: [{ name: topicName, partitions: [0] }],
        });
        await context.cluster.sendRequestToNode(context.leaderId)(API.PRODUCE, {
            transactionalId,
            acks: -1,
            timeoutMs: 10_000,
            topicData: [
                {
                    name: topicName,
                    topicId: context.topicId,
                    partitionData: [
                        {
                            ...createPartitionData('transactional'),
                            attributes: 0x10,
                            producerId,
                            producerEpoch,
                            baseSequence: 0,
                        },
                    ],
                },
            ],
        });
        return transaction;
    };

    describe('AddPartitionsToTxn', () => {
        forEachVersion(API.ADD_PARTITIONS_TO_TXN, async (api) => {
            const { transactionalId, sendRequest, producerId, producerEpoch } = await beginTransaction(
                `add-partitions-v${api.apiVersion}`,
            );
            const topics = [{ name: topicName, partitions: [0] }];
            const response = await sendRequest(
                api,
                api.apiVersion < 4
                    ? {
                          v3AndBelowTransactionalId: transactionalId,
                          v3AndBelowProducerId: producerId,
                          v3AndBelowProducerEpoch: producerEpoch,
                          v3AndBelowTopics: topics,
                      }
                    : { transactions: [{ transactionalId, producerId, producerEpoch, verifyOnly: false, topics }] },
            );
            const results =
                api.apiVersion < 4 ? response.resultsByTopicV3AndBelow : response.resultsByTransaction[0].topicResults;
            expect(results).toEqual([
                expect.objectContaining({
                    name: topicName,
                    resultsByPartition: [expect.objectContaining({ partitionIndex: 0, partitionErrorCode: 0 })],
                }),
            ]);
        });
    });

    describe('AddOffsetsToTxn', () => {
        forEachVersion(API.ADD_OFFSETS_TO_TXN, async (api) => {
            const { transactionalId, sendRequest, producerId, producerEpoch } = await beginTransaction(
                `add-offsets-v${api.apiVersion}`,
            );
            const { errorCode } = await sendRequest(api, {
                transactionalId,
                producerId,
                producerEpoch,
                groupId: randomName('transactional-group'),
            });
            expect(errorCode).toBe(0);
        });
    });

    describe('TxnOffsetCommit', () => {
        forEachVersion(API.TXN_OFFSET_COMMIT, async (api) => {
            const { transactionalId, sendRequest, producerId, producerEpoch } = await beginTransaction(
                `txn-offset-commit-v${api.apiVersion}`,
            );
            const groupId = randomName('transactional-group');
            await sendRequest(API.ADD_OFFSETS_TO_TXN, { transactionalId, producerId, producerEpoch, groupId });

            const sendToGroupCoordinator = await findCoordinator(groupId);
            const { topics } = await sendToGroupCoordinator(api, {
                transactionalId,
                groupId,
                producerId,
                producerEpoch,
                generationId: -1,
                memberId: '',
                groupInstanceId: null,
                topics: [
                    {
                        name: topicName,
                        partitions: [
                            {
                                partitionIndex: 0,
                                committedOffset: 1n,
                                committedLeaderEpoch: -1,
                                committedMetadata: null,
                            },
                        ],
                    },
                ],
            });
            expect(topics).toEqual([
                expect.objectContaining({ name: topicName, partitions: [expect.objectContaining({ errorCode: 0 })] }),
            ]);
        });
    });

    describe('EndTxn', () => {
        forEachVersion(API.END_TXN, async (api) => {
            const { transactionalId, sendRequest, producerId, producerEpoch } = await beginTransactionWithPartition(
                `end-txn-v${api.apiVersion}`,
            );
            const { errorCode } = await sendRequest(api, {
                transactionalId,
                producerId,
                producerEpoch,
                committed: false,
            });
            expect(errorCode).toBe(0);
        });
    });

    describe('DescribeTransactions', () => {
        forEachVersion(API.DESCRIBE_TRANSACTIONS, async (api) => {
            const { transactionalId, sendRequest, producerId } = await beginTransactionWithPartition(
                `describe-txn-v${api.apiVersion}`,
            );
            const { transactionStates } = await sendRequest(api, { transactionalIds: [transactionalId] });
            expect(transactionStates).toEqual([
                expect.objectContaining({
                    transactionalId,
                    producerId,
                    transactionState: 'Ongoing',
                    topics: [expect.objectContaining({ topic: topicName, partitions: [0] })],
                }),
            ]);
        });
    });

    describe('ListTransactions', () => {
        forEachVersion(API.LIST_TRANSACTIONS, async (api) => {
            const { transactionalId, sendRequest, producerId } = await beginTransactionWithPartition(
                `list-txn-v${api.apiVersion}`,
            );
            const { transactionStates } = await sendRequest(api, {
                stateFilters: ['Ongoing'],
                producerIdFilters: [producerId],
                durationFilter: -1n,
                transactionalIdPattern: null,
            });
            expect(transactionStates).toEqual([
                expect.objectContaining({ transactionalId, producerId, transactionState: 'Ongoing' }),
            ]);
        });
    });

    describe('DescribeProducers', () => {
        forEachVersion(API.DESCRIBE_PRODUCERS, async (api) => {
            const { producerId } = await beginTransactionWithPartition(`describe-producers-v${api.apiVersion}`);
            const { topics } = await context.cluster.sendRequestToNode(context.leaderId)(api, {
                topics: [{ name: topicName, partitionIndexes: [0] }],
            });
            expect(topics[0].partitions[0].activeProducers).toContainEqual(expect.objectContaining({ producerId }));
        });
    });
});
