import { createApi } from '../../utils/api';
import { DescribeClientQuotasRequest, DescribeClientQuotasResponse, throwIfError } from './common';

/*
DescribeClientQuotas Request (Version: 0) => { [components] strict }
  components => { entity_type match_type match }
    entity_type => STRING
    match_type => INT8
    match => NULLABLE_STRING
  strict => BOOLEAN

DescribeClientQuotas Response (Version: 0) => { throttle_time_ms error_code error_message ?[entries] }
  throttle_time_ms => INT32
  error_code => INT16
  error_message => NULLABLE_STRING
  entries => { [entity] [values] }
    entity => { entity_type entity_name }
      entity_type => STRING
      entity_name => NULLABLE_STRING
    values => { key value }
      key => STRING
      value => FLOAT64
*/
export const DESCRIBE_CLIENT_QUOTAS_V0 = createApi<DescribeClientQuotasRequest, DescribeClientQuotasResponse>({
    apiKey: 48,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeArray(data.components, (encoder, component) =>
                encoder.writeString(component.entityType).writeInt8(component.matchType).writeString(component.match),
            )
            .writeBoolean(data.strict),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            errorCode: decoder.readInt16(),
            errorMessage: decoder.readString(),
            entries: decoder.readArray((entry) => ({
                entity: entry.readArray((entity) => ({
                    entityType: entity.readString()!,
                    entityName: entity.readString(),
                    tags: {},
                })),
                values: entry.readArray((value) => ({
                    key: value.readString()!,
                    value: value.readFloat64(),
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        }),
});
