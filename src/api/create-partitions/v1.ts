import { createApi } from '../../utils/api';
import { CREATE_PARTITIONS_V0 } from './v0';

/*
CreatePartitions Request (Version: 1) => { [topics] timeout_ms validate_only }
  topics => { name count ?[assignments] }
    name => STRING
    count => INT32
    assignments => { [broker_ids] }
      broker_ids => INT32
  timeout_ms => INT32
  validate_only => BOOLEAN

CreatePartitions Response (Version: 1) => { throttle_time_ms [results] }
  throttle_time_ms => INT32
  results => { name error_code error_message }
    name => STRING
    error_code => INT16
    error_message => NULLABLE_STRING
*/
export const CREATE_PARTITIONS_V1 = createApi({
    ...CREATE_PARTITIONS_V0,
    apiVersion: 1,
    fallback: CREATE_PARTITIONS_V0,
});
