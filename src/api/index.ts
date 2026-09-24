import { Api } from '../utils/api';
import { delay } from '../utils/delay';
import { KafkaTSApiError } from '../utils/error';
import { log } from '../utils/logger';
import { ADD_OFFSETS_TO_TXN } from './add-offsets-to-txn';
import { ADD_PARTITIONS_TO_TXN } from './add-partitions-to-txn';
import { ADD_RAFT_VOTER } from './add-raft-voter';
import { ALTER_CLIENT_QUOTAS } from './alter-client-quotas';
import { ALTER_CONFIGS } from './alter-configs';
import { ALTER_PARTITION_REASSIGNMENTS } from './alter-partition-reassignments';
import { ALTER_REPLICA_LOG_DIRS } from './alter-replica-log-dirs';
import { ALTER_SHARE_GROUP_OFFSETS } from './alter-share-group-offsets';
import { ALTER_USER_SCRAM_CREDENTIALS } from './alter-user-scram-credentials';
import { API_VERSIONS } from './api-versions';
import { CONSUMER_GROUP_DESCRIBE } from './consumer-group-describe';
import { CONSUMER_GROUP_HEARTBEAT } from './consumer-group-heartbeat';
import { CREATE_ACLS } from './create-acls';
import { CREATE_DELEGATION_TOKEN } from './create-delegation-token';
import { CREATE_PARTITIONS } from './create-partitions';
import { CREATE_TOPICS } from './create-topics';
import { DELETE_ACLS } from './delete-acls';
import { DELETE_GROUPS } from './delete-groups';
import { DELETE_RECORDS } from './delete-records';
import { DELETE_SHARE_GROUP_OFFSETS } from './delete-share-group-offsets';
import { DELETE_SHARE_GROUP_STATE } from './delete-share-group-state';
import { DELETE_TOPICS } from './delete-topics';
import { DESCRIBE_ACLS } from './describe-acls';
import { DESCRIBE_CLIENT_QUOTAS } from './describe-client-quotas';
import { DESCRIBE_CLUSTER } from './describe-cluster';
import { DESCRIBE_CONFIGS } from './describe-configs';
import { DESCRIBE_DELEGATION_TOKEN } from './describe-delegation-token';
import { DESCRIBE_GROUPS } from './describe-groups';
import { DESCRIBE_LOG_DIRS } from './describe-log-dirs';
import { DESCRIBE_PRODUCERS } from './describe-producers';
import { DESCRIBE_QUORUM } from './describe-quorum';
import { DESCRIBE_SHARE_GROUP_OFFSETS } from './describe-share-group-offsets';
import { DESCRIBE_TOPIC_PARTITIONS } from './describe-topic-partitions';
import { DESCRIBE_TRANSACTIONS } from './describe-transactions';
import { DESCRIBE_USER_SCRAM_CREDENTIALS } from './describe-user-scram-credentials';
import { ELECT_LEADERS } from './elect-leaders';
import { END_TXN } from './end-txn';
import { EXPIRE_DELEGATION_TOKEN } from './expire-delegation-token';
import { FETCH } from './fetch';
import { FIND_COORDINATOR } from './find-coordinator';
import { GET_TELEMETRY_SUBSCRIPTIONS } from './get-telemetry-subscriptions';
import { HEARTBEAT } from './heartbeat';
import { INCREMENTAL_ALTER_CONFIGS } from './incremental-alter-configs';
import { INIT_PRODUCER_ID } from './init-producer-id';
import { INITIALIZE_SHARE_GROUP_STATE } from './initialize-share-group-state';
import { JOIN_GROUP } from './join-group';
import { LEAVE_GROUP } from './leave-group';
import { LIST_CONFIG_RESOURCES } from './list-config-resources';
import { LIST_GROUPS } from './list-groups';
import { LIST_OFFSETS } from './list-offsets';
import { LIST_PARTITION_REASSIGNMENTS } from './list-partition-reassignments';
import { LIST_TRANSACTIONS } from './list-transactions';
import { METADATA } from './metadata';
import { OFFSET_COMMIT } from './offset-commit';
import { OFFSET_DELETE } from './offset-delete';
import { OFFSET_FETCH } from './offset-fetch';
import { OFFSET_FOR_LEADER_EPOCH } from './offset-for-leader-epoch';
import { PRODUCE } from './produce';
import { PUSH_TELEMETRY } from './push-telemetry';
import { READ_SHARE_GROUP_STATE } from './read-share-group-state';
import { READ_SHARE_GROUP_STATE_SUMMARY } from './read-share-group-state-summary';
import { REMOVE_RAFT_VOTER } from './remove-raft-voter';
import { RENEW_DELEGATION_TOKEN } from './renew-delegation-token';
import { SASL_AUTHENTICATE } from './sasl-authenticate';
import { SASL_HANDSHAKE } from './sasl-handshake';
import { SHARE_ACKNOWLEDGE } from './share-acknowledge';
import { SHARE_FETCH } from './share-fetch';
import { SHARE_GROUP_DESCRIBE } from './share-group-describe';
import { SHARE_GROUP_HEARTBEAT } from './share-group-heartbeat';
import { STREAMS_GROUP_DESCRIBE } from './streams-group-describe';
import { STREAMS_GROUP_HEARTBEAT } from './streams-group-heartbeat';
import { SYNC_GROUP } from './sync-group';
import { TXN_OFFSET_COMMIT } from './txn-offset-commit';
import { UNREGISTER_BROKER } from './unregister-broker';
import { UPDATE_FEATURES } from './update-features';
import { WRITE_SHARE_GROUP_STATE } from './write-share-group-state';
import { WRITE_TXN_MARKERS } from './write-txn-markers';

export const API = {
    ADD_OFFSETS_TO_TXN,
    ADD_PARTITIONS_TO_TXN,
    ADD_RAFT_VOTER,
    ALTER_CLIENT_QUOTAS,
    ALTER_CONFIGS,
    ALTER_PARTITION_REASSIGNMENTS,
    ALTER_REPLICA_LOG_DIRS,
    ALTER_SHARE_GROUP_OFFSETS,
    ALTER_USER_SCRAM_CREDENTIALS,
    API_VERSIONS,
    CONSUMER_GROUP_DESCRIBE,
    CONSUMER_GROUP_HEARTBEAT,
    CREATE_ACLS,
    CREATE_DELEGATION_TOKEN,
    CREATE_PARTITIONS,
    CREATE_TOPICS,
    DELETE_ACLS,
    DELETE_GROUPS,
    DELETE_RECORDS,
    DELETE_SHARE_GROUP_OFFSETS,
    DELETE_SHARE_GROUP_STATE,
    DELETE_TOPICS,
    DESCRIBE_ACLS,
    DESCRIBE_CLIENT_QUOTAS,
    DESCRIBE_CLUSTER,
    DESCRIBE_CONFIGS,
    DESCRIBE_DELEGATION_TOKEN,
    DESCRIBE_GROUPS,
    DESCRIBE_LOG_DIRS,
    DESCRIBE_PRODUCERS,
    DESCRIBE_QUORUM,
    DESCRIBE_SHARE_GROUP_OFFSETS,
    DESCRIBE_TOPIC_PARTITIONS,
    DESCRIBE_TRANSACTIONS,
    DESCRIBE_USER_SCRAM_CREDENTIALS,
    ELECT_LEADERS,
    END_TXN,
    EXPIRE_DELEGATION_TOKEN,
    FETCH,
    FIND_COORDINATOR,
    GET_TELEMETRY_SUBSCRIPTIONS,
    HEARTBEAT,
    INCREMENTAL_ALTER_CONFIGS,
    INITIALIZE_SHARE_GROUP_STATE,
    INIT_PRODUCER_ID,
    JOIN_GROUP,
    LEAVE_GROUP,
    LIST_CONFIG_RESOURCES,
    LIST_GROUPS,
    LIST_OFFSETS,
    LIST_PARTITION_REASSIGNMENTS,
    LIST_TRANSACTIONS,
    METADATA,
    OFFSET_COMMIT,
    OFFSET_DELETE,
    OFFSET_FETCH,
    OFFSET_FOR_LEADER_EPOCH,
    PRODUCE,
    PUSH_TELEMETRY,
    READ_SHARE_GROUP_STATE,
    READ_SHARE_GROUP_STATE_SUMMARY,
    REMOVE_RAFT_VOTER,
    RENEW_DELEGATION_TOKEN,
    SASL_AUTHENTICATE,
    SASL_HANDSHAKE,
    SHARE_ACKNOWLEDGE,
    SHARE_FETCH,
    SHARE_GROUP_DESCRIBE,
    SHARE_GROUP_HEARTBEAT,
    STREAMS_GROUP_DESCRIBE,
    STREAMS_GROUP_HEARTBEAT,
    SYNC_GROUP,
    TXN_OFFSET_COMMIT,
    UNREGISTER_BROKER,
    UPDATE_FEATURES,
    WRITE_SHARE_GROUP_STATE,
    WRITE_TXN_MARKERS,
};

const apiNameByKey = Object.fromEntries(Object.entries(API).map(([k, v]) => [v.apiKey, k]));

export const getApiName = <Request, Response>(api: Api<Request, Response>) => apiNameByKey[api.apiKey];

export const API_ERROR = {
    UNKNOWN_SERVER_ERROR: -1,
    OFFSET_OUT_OF_RANGE: 1,
    CORRUPT_MESSAGE: 2,
    UNKNOWN_TOPIC_OR_PARTITION: 3,
    INVALID_FETCH_SIZE: 4,
    LEADER_NOT_AVAILABLE: 5,
    NOT_LEADER_OR_FOLLOWER: 6,
    REQUEST_TIMED_OUT: 7,
    BROKER_NOT_AVAILABLE: 8,
    REPLICA_NOT_AVAILABLE: 9,
    MESSAGE_TOO_LARGE: 10,
    STALE_CONTROLLER_EPOCH: 11,
    OFFSET_METADATA_TOO_LARGE: 12,
    NETWORK_EXCEPTION: 13,
    COORDINATOR_LOAD_IN_PROGRESS: 14,
    COORDINATOR_NOT_AVAILABLE: 15,
    NOT_COORDINATOR: 16,
    INVALID_TOPIC_EXCEPTION: 17,
    RECORD_LIST_TOO_LARGE: 18,
    NOT_ENOUGH_REPLICAS: 19,
    NOT_ENOUGH_REPLICAS_AFTER_APPEND: 20,
    INVALID_REQUIRED_ACKS: 21,
    ILLEGAL_GENERATION: 22,
    INCONSISTENT_GROUP_PROTOCOL: 23,
    INVALID_GROUP_ID: 24,
    UNKNOWN_MEMBER_ID: 25,
    INVALID_SESSION_TIMEOUT: 26,
    REBALANCE_IN_PROGRESS: 27,
    INVALID_COMMIT_OFFSET_SIZE: 28,
    TOPIC_AUTHORIZATION_FAILED: 29,
    GROUP_AUTHORIZATION_FAILED: 30,
    CLUSTER_AUTHORIZATION_FAILED: 31,
    INVALID_TIMESTAMP: 32,
    UNSUPPORTED_SASL_MECHANISM: 33,
    ILLEGAL_SASL_STATE: 34,
    UNSUPPORTED_VERSION: 35,
    TOPIC_ALREADY_EXISTS: 36,
    INVALID_PARTITIONS: 37,
    INVALID_REPLICATION_FACTOR: 38,
    INVALID_REPLICA_ASSIGNMENT: 39,
    INVALID_CONFIG: 40,
    NOT_CONTROLLER: 41,
    INVALID_REQUEST: 42,
    UNSUPPORTED_FOR_MESSAGE_FORMAT: 43,
    POLICY_VIOLATION: 44,
    OUT_OF_ORDER_SEQUENCE_NUMBER: 45,
    DUPLICATE_SEQUENCE_NUMBER: 46,
    INVALID_PRODUCER_EPOCH: 47,
    INVALID_TXN_STATE: 48,
    INVALID_PRODUCER_ID_MAPPING: 49,
    INVALID_TRANSACTION_TIMEOUT: 50,
    CONCURRENT_TRANSACTIONS: 51,
    TRANSACTION_COORDINATOR_FENCED: 52,
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED: 53,
    SECURITY_DISABLED: 54,
    OPERATION_NOT_ATTEMPTED: 55,
    KAFKA_STORAGE_ERROR: 56,
    LOG_DIR_NOT_FOUND: 57,
    SASL_AUTHENTICATION_FAILED: 58,
    UNKNOWN_PRODUCER_ID: 59,
    REASSIGNMENT_IN_PROGRESS: 60,
    DELEGATION_TOKEN_AUTH_DISABLED: 61,
    DELEGATION_TOKEN_NOT_FOUND: 62,
    DELEGATION_TOKEN_OWNER_MISMATCH: 63,
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: 64,
    DELEGATION_TOKEN_AUTHORIZATION_FAILED: 65,
    DELEGATION_TOKEN_EXPIRED: 66,
    INVALID_PRINCIPAL_TYPE: 67,
    NON_EMPTY_GROUP: 68,
    GROUP_ID_NOT_FOUND: 69,
    FETCH_SESSION_ID_NOT_FOUND: 70,
    INVALID_FETCH_SESSION_EPOCH: 71,
    LISTENER_NOT_FOUND: 72,
    TOPIC_DELETION_DISABLED: 73,
    FENCED_LEADER_EPOCH: 74,
    UNKNOWN_LEADER_EPOCH: 75,
    UNSUPPORTED_COMPRESSION_TYPE: 76,
    STALE_BROKER_EPOCH: 77,
    OFFSET_NOT_AVAILABLE: 78,
    MEMBER_ID_REQUIRED: 79,
    PREFERRED_LEADER_NOT_AVAILABLE: 80,
    GROUP_MAX_SIZE_REACHED: 81,
    FENCED_INSTANCE_ID: 82,
    ELIGIBLE_LEADERS_NOT_AVAILABLE: 83,
    ELECTION_NOT_NEEDED: 84,
    NO_REASSIGNMENT_IN_PROGRESS: 85,
    GROUP_SUBSCRIBED_TO_TOPIC: 86,
    INVALID_RECORD: 87,
    UNSTABLE_OFFSET_COMMIT: 88,
    THROTTLING_QUOTA_EXCEEDED: 89,
    PRODUCER_FENCED: 90,
    RESOURCE_NOT_FOUND: 91,
    DUPLICATE_RESOURCE: 92,
    UNACCEPTABLE_CREDENTIAL: 93,
    INCONSISTENT_VOTER_SET: 94,
    INVALID_UPDATE_VERSION: 95,
    FEATURE_UPDATE_FAILED: 96,
    PRINCIPAL_DESERIALIZATION_FAILURE: 97,
    SNAPSHOT_NOT_FOUND: 98,
    POSITION_OUT_OF_RANGE: 99,
    UNKNOWN_TOPIC_ID: 100,
    DUPLICATE_BROKER_REGISTRATION: 101,
    BROKER_ID_NOT_REGISTERED: 102,
    INCONSISTENT_TOPIC_ID: 103,
    INCONSISTENT_CLUSTER_ID: 104,
    TRANSACTIONAL_ID_NOT_FOUND: 105,
    FETCH_SESSION_TOPIC_ID_ERROR: 106,
    INELIGIBLE_REPLICA: 107,
    NEW_LEADER_ELECTED: 108,
    OFFSET_MOVED_TO_TIERED_STORAGE: 109,
    FENCED_MEMBER_EPOCH: 110,
    UNRELEASED_INSTANCE_ID: 111,
    UNSUPPORTED_ASSIGNOR: 112,
    STALE_MEMBER_EPOCH: 113,
    MISMATCHED_ENDPOINT_TYPE: 114,
    UNSUPPORTED_ENDPOINT_TYPE: 115,
    UNKNOWN_CONTROLLER_ID: 116,
    UNKNOWN_SUBSCRIPTION_ID: 117,
    TELEMETRY_TOO_LARGE: 118,
    INVALID_REGISTRATION: 119,
    TRANSACTION_ABORTABLE: 120,
} as const;

export const handleApiError = async (error: unknown) => {
    if (error instanceof KafkaTSApiError) {
        switch (error.errorCode) {
            case API_ERROR.LEADER_NOT_AVAILABLE:
                log.debug('Leader not available yet. Retrying...');
                return delay(500);
            case API_ERROR.COORDINATOR_LOAD_IN_PROGRESS:
                log.debug('Coordinator load in progress. Retrying...');
                return delay(100);
            case API_ERROR.COORDINATOR_NOT_AVAILABLE:
                log.debug('Coordinator not available yet. Retrying...');
                return delay(100);
            case API_ERROR.OFFSET_NOT_AVAILABLE:
                log.debug('Offset not available yet. Retrying...');
                return delay(100);
        }
    }
    throw error;
};
