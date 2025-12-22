import { API } from '../api';
import { SASLProvider } from '../broker';

export const saslPlain = ({ username, password }: { username: string; password: string }): SASLProvider => ({
    mechanism: 'PLAIN',
    authenticate: async ({ sendRequest }) => {
        const authBytes = [null, username, password].join('\u0000');
        await sendRequest(API.SASL_AUTHENTICATE, { authBytes: Buffer.from(authBytes) });
    },
});
