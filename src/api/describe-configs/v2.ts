import { createApi } from '../../utils/api';
import { DESCRIBE_CONFIGS_V1 } from './v1';

/*
DescribeConfigs Request (Version: 2) => { [resources] include_synonyms }
  resources => { resource_type resource_name ?[configuration_keys] }
    resource_type => INT8
    resource_name => STRING
    configuration_keys => STRING
  include_synonyms => BOOLEAN

DescribeConfigs Response (Version: 2) => { throttle_time_ms [results] }
  throttle_time_ms => INT32
  results => { error_code error_message resource_type resource_name [configs] }
    error_code => INT16
    error_message => NULLABLE_STRING
    resource_type => INT8
    resource_name => STRING
    configs => { name value read_only config_source is_sensitive [synonyms] }
      name => STRING
      value => NULLABLE_STRING
      read_only => BOOLEAN
      config_source => INT8
      is_sensitive => BOOLEAN
      synonyms => { name value source }
        name => STRING
        value => NULLABLE_STRING
        source => INT8
*/
export const DESCRIBE_CONFIGS_V2 = createApi({ ...DESCRIBE_CONFIGS_V1, apiVersion: 2, fallback: DESCRIBE_CONFIGS_V1 });
