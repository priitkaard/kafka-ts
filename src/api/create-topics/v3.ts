import { createApi } from '../../utils/api';
import { CREATE_TOPICS_V2 } from './v2';

/*
CreateTopics Request (Version: 3) => { [topics] timeout_ms validate_only }
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

CreateTopics Response (Version: 3) => { throttle_time_ms [topics] }
  throttle_time_ms => INT32
  topics => { name error_code error_message }
    name => STRING
    error_code => INT16
    error_message => NULLABLE_STRING
*/
export const CREATE_TOPICS_V3 = createApi({ ...CREATE_TOPICS_V2, apiVersion: 3, fallback: CREATE_TOPICS_V2 });
