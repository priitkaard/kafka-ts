import { createApi } from '../../utils/api';
import { SASL_HANDSHAKE_V0 } from './v0';

/*
SaslHandshake Request (Version: 1) => { mechanism }
  mechanism => STRING

SaslHandshake Response (Version: 1) => { error_code [mechanisms] }
  error_code => INT16
  mechanisms => STRING
*/
export const SASL_HANDSHAKE_V1 = createApi({ ...SASL_HANDSHAKE_V0, apiVersion: 1, fallback: SASL_HANDSHAKE_V0 });
