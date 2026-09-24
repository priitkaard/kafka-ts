import { createApi } from '../../utils/api';
import { ALTER_CONFIGS_V0 } from './v0';

/*
AlterConfigs Request (Version: 1) => { [resources] validate_only }
  resources => { resource_type resource_name [configs] }
    resource_type => INT8
    resource_name => STRING
    configs => { name value }
      name => STRING
      value => NULLABLE_STRING
  validate_only => BOOLEAN

AlterConfigs Response (Version: 1) => { throttle_time_ms [responses] }
  throttle_time_ms => INT32
  responses => { error_code error_message resource_type resource_name }
    error_code => INT16
    error_message => NULLABLE_STRING
    resource_type => INT8
    resource_name => STRING
*/
export const ALTER_CONFIGS_V1 = createApi({ ...ALTER_CONFIGS_V0, apiVersion: 1, fallback: ALTER_CONFIGS_V0 });
