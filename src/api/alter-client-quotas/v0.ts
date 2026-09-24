import { createApi } from '../../utils/api';
import { AlterClientQuotasRequest, AlterClientQuotasResponse, throwIfError } from './common';

/*
AlterClientQuotas Request (Version: 0) => { [entries] validate_only }
  entries => { [entity] [ops] }
    entity => { entity_type entity_name }
      entity_type => STRING
      entity_name => NULLABLE_STRING
    ops => { key value remove }
      key => STRING
      value => FLOAT64
      remove => BOOLEAN
  validate_only => BOOLEAN

AlterClientQuotas Response (Version: 0) => { throttle_time_ms [entries] }
  throttle_time_ms => INT32
  entries => { error_code error_message [entity] }
    error_code => INT16
    error_message => NULLABLE_STRING
    entity => { entity_type entity_name }
      entity_type => STRING
      entity_name => NULLABLE_STRING
*/
export const ALTER_CLIENT_QUOTAS_V0 = createApi<AlterClientQuotasRequest, AlterClientQuotasResponse>({
    apiKey: 49,
    apiVersion: 0,
    requestHeaderVersion: 1,
    responseHeaderVersion: 0,
    request: (encoder, data) =>
        encoder
            .writeArray(data.entries, (encoder, entry) =>
                encoder
                    .writeArray(entry.entity, (encoder, entity) =>
                        encoder.writeString(entity.entityType).writeString(entity.entityName),
                    )
                    .writeArray(entry.ops, (encoder, op) =>
                        encoder.writeString(op.key).writeFloat64(op.value).writeBoolean(op.remove),
                    ),
            )
            .writeBoolean(data.validateOnly),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            entries: decoder.readArray((entry) => ({
                errorCode: entry.readInt16(),
                errorMessage: entry.readString(),
                entity: entry.readArray((entity) => ({
                    entityType: entity.readString()!,
                    entityName: entity.readString(),
                    tags: {},
                })),
                tags: {},
            })),
            tags: {},
        }),
});
