import { createApi } from '../../utils/api';
import { throwIfError } from './common';
import { ALTER_CLIENT_QUOTAS_V0 } from './v0';

/*
AlterClientQuotas Request (Version: 1) => { (entries) validate_only }
  entries => { (entity) (ops) }
    entity => { entity_type entity_name }
      entity_type => COMPACT_STRING
      entity_name => COMPACT_NULLABLE_STRING
    ops => { key value remove }
      key => COMPACT_STRING
      value => FLOAT64
      remove => BOOLEAN
  validate_only => BOOLEAN

AlterClientQuotas Response (Version: 1) => { throttle_time_ms (entries) }
  throttle_time_ms => INT32
  entries => { error_code error_message (entity) }
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    entity => { entity_type entity_name }
      entity_type => COMPACT_STRING
      entity_name => COMPACT_NULLABLE_STRING
*/
export const ALTER_CLIENT_QUOTAS_V1 = createApi({
    ...ALTER_CLIENT_QUOTAS_V0,
    apiVersion: 1,
    fallback: ALTER_CLIENT_QUOTAS_V0,
    requestHeaderVersion: 2,
    responseHeaderVersion: 1,
    request: (encoder, data) =>
        encoder
            .writeCompactArray(data.entries, (encoder, entry) =>
                encoder
                    .writeCompactArray(entry.entity, (encoder, entity) =>
                        encoder
                            .writeCompactString(entity.entityType)
                            .writeCompactString(entity.entityName)
                            .writeTagBuffer(),
                    )
                    .writeCompactArray(entry.ops, (encoder, op) =>
                        encoder
                            .writeCompactString(op.key)
                            .writeFloat64(op.value)
                            .writeBoolean(op.remove)
                            .writeTagBuffer(),
                    )
                    .writeTagBuffer(),
            )
            .writeBoolean(data.validateOnly)
            .writeTagBuffer(),
    response: (decoder) =>
        throwIfError({
            throttleTimeMs: decoder.readInt32(),
            entries: decoder.readCompactArray((entry) => ({
                errorCode: entry.readInt16(),
                errorMessage: entry.readCompactString(),
                entity: entry.readCompactArray((entity) => ({
                    entityType: entity.readCompactString()!,
                    entityName: entity.readCompactString(),
                    tags: entity.readTagBuffer(),
                })),
                tags: entry.readTagBuffer(),
            })),
            tags: decoder.readTagBuffer(),
        }),
});
