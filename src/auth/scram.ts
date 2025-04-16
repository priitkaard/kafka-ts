import { API } from '../api';
import { SASLProvider } from '../broker';
import { base64Decode, base64Encode, generateNonce, hash, hmac, saltPassword, xor } from '../utils/crypto';
import { KafkaTSError } from '../utils/error';

const saslScram =
    ({ mechanism, keyLength, digest }: { mechanism: string; keyLength: number; digest: string }) =>
    ({ username, password }: { username: string; password: string }): SASLProvider => ({
        mechanism,
        authenticate: async ({ sendRequest }) => {
            const nonce = generateNonce();
            const firstMessage = `n=${username},r=${nonce}`;

            const { authBytes } = await sendRequest(API.SASL_AUTHENTICATE, {
                authBytes: Buffer.from(`n,,${firstMessage}`),
            });
            if (!authBytes) {
                throw new KafkaTSError('No auth response');
            }

            const response = Object.fromEntries(
                authBytes
                    .toString()
                    .split(',')
                    .map((pair) => pair.split('=')),
            ) as { r: string; s: string; i: string };

            const rnonce = response.r;
            if (!rnonce.startsWith(nonce)) {
                throw new KafkaTSError('Invalid nonce');
            }
            const iterations = parseInt(response.i);
            const salt = base64Decode(response.s);

            const saltedPassword = await saltPassword(password, salt, iterations, keyLength, digest);
            const clientKey = hmac(saltedPassword, 'Client Key', digest);
            const clientKeyHash = hash(clientKey, digest);

            let finalMessage = `c=${base64Encode(Buffer.from('n,,'))},r=${rnonce}`;

            const fullMessage = `${firstMessage},${authBytes.toString()},${finalMessage}`;
            const clientSignature = hmac(clientKeyHash, fullMessage, digest);
            const clientProof = base64Encode(xor(clientKey, clientSignature));

            finalMessage += `,p=${clientProof}`;

            await sendRequest(API.SASL_AUTHENTICATE, { authBytes: Buffer.from(finalMessage) });
        },
    });

export const saslScramSha256 = saslScram({ mechanism: 'SCRAM-SHA-256', keyLength: 32, digest: 'sha256' });
export const saslScramSha512 = saslScram({ mechanism: 'SCRAM-SHA-512', keyLength: 64, digest: 'sha512' });
