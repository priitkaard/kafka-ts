import { createApi } from '../../utils/api';
import { CREATE_TOPICS_V3 } from './v3';

/*
CreateTopics Request (Version: 4) => { [topics] timeout_ms validate_only }
  topics => { name num_partitions replication_factor [assignments] [configs] }
    name => STRING
    num_partitions => INT32
    replication_factor => INT16
    assignments => { partition_index [broker_ids] }
      partition_index => INT32
      broker_ids => INT32
    configs => { name value }
      name => STRING
      value => NULLABLE_STRING
  timeout_ms => INT32
  validate_only => BOOLEAN

CreateTopics Response (Version: 4) => { throttle_time_ms [topics] }
  throttle_time_ms => INT32
  topics => { name error_code error_message }
    name => STRING
    error_code => INT16
    error_message => NULLABLE_STRING
*/
export const CREATE_TOPICS_V4 = createApi({ ...CREATE_TOPICS_V3, apiVersion: 4, fallback: CREATE_TOPICS_V3 });
