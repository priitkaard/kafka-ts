import { createApi } from '../../utils/api';
import { CREATE_PARTITIONS_V2 } from './v2';

/*
CreatePartitions Request (Version: 3) => { (topics) timeout_ms validate_only }
  topics => { name count ?(assignments) }
    name => COMPACT_STRING
    count => INT32
    assignments => { (broker_ids) }
      broker_ids => INT32
  timeout_ms => INT32
  validate_only => BOOLEAN

CreatePartitions Response (Version: 3) => { throttle_time_ms (results) }
  throttle_time_ms => INT32
  results => { name error_code error_message }
    name => COMPACT_STRING
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
*/
export const CREATE_PARTITIONS_V3 = createApi({
    ...CREATE_PARTITIONS_V2,
    apiVersion: 3,
    fallback: CREATE_PARTITIONS_V2,
});
