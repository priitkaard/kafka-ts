import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { DESCRIBE_CLIENT_QUOTAS_V0 } from './v0';

/*
DescribeClientQuotas Request (Version: 1) => { (components) strict }
  components => { entity_type match_type match }
    entity_type => COMPACT_STRING
    match_type => INT8
    match => COMPACT_NULLABLE_STRING
  strict => BOOLEAN

DescribeClientQuotas Response (Version: 1) => { throttle_time_ms error_code error_message ?(entries) }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => COMPACT_NULLABLE_STRING
  entries => { (entity) (values) }
    entity => { entity_type entity_name }
      entity_type => COMPACT_STRING
      entity_name => COMPACT_NULLABLE_STRING
    values => { key value }
      key => COMPACT_STRING
      value => FLOAT64
*/
export const DESCRIBE_CLIENT_QUOTAS_V1 = createApi({
    ...DESCRIBE_CLIENT_QUOTAS_V0,
    apiVersion: 1,
    fallback: DESCRIBE_CLIENT_QUOTAS_V0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.components, (encoder, component) =>
                encoder
                    .writeCompactString(component.entityType)
                    .writeInt8(component.matchType)
                    .writeCompactString(component.match)
                    .writeTagBuffer(),
            )
            .writeBoolean(data.strict)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readCompactString(),
            entries: decoder.readCompactArray((entry) => ({
                entity: entry.readCompactArray((entity) => ({
                    entityType: entity.readCompactString()!,
                    entityName: entity.readCompactString(),
                    tags: entity.readTagBuffer(),
                })),
                values: entry.readCompactArray((value) => ({
                    key: value.readCompactString()!,
                    value: value.readFloat64(),
                    tags: value.readTagBuffer(),
                })),
                tags: entry.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
