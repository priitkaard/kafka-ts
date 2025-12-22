import { API } from '../api';
import { SASLProvider } from '../broker';
import { clamp } from '../utils/number';
import { exponentialBackoff, withRetry } from '../utils/retry';

const MAX_INT = Math.pow(2, 31) - 1;

export const oAuthBearer = (getToken: () => Promise<{ access_token: string }>): SASLProvider => {
    return {
        mechanism: 'OAUTHBEARER',
        authenticate: async ({ sendRequest }) => {
            const { access_token: accessToken } = await getToken();

            const sep = String.fromCharCode(1);
            const authBytes = `n,,${sep}auth=Bearer ${accessToken}${sep}${sep}`;
            await sendRequest(API.SASL_AUTHENTICATE, { authBytes: Buffer.from(authBytes) });
        },
    };
};

export const oAuthAuthenticator = ({
    endpoint,
    clientId,
    clientSecret,
    refreshThresholdSeconds = 15,
}: {
    endpoint: string;
    clientId: string;
    clientSecret: string;
    refreshThresholdSeconds?: number;
}) => {
    let tokenPromise = createToken(endpoint, {
        grant_type: 'client_credentials',
        client_id: clientId,
        client_secret: clientSecret,
    });

    const scheduleRefresh = () => {
        tokenPromise.then((token) => {
            const refreshInMs = clamp((token.expires_in - refreshThresholdSeconds) * 1000, 1, MAX_INT);
            setTimeout(() => {
                tokenPromise = createToken(endpoint, {
                    grant_type: 'refresh_token',
                    client_id: clientId,
                    client_secret: clientSecret,
                    refresh_token: token.refresh_token,
                });
                scheduleRefresh();
            }, refreshInMs);
        });
    };
    scheduleRefresh();

    return () => tokenPromise;
};

type TokenRequest = {
    client_id: string;
    client_secret: string;
} & ({ grant_type: 'client_credentials' } | { grant_type: 'refresh_token'; refresh_token: string });

type TokenResponse = {
    access_token: string;
    refresh_token: string;
    expires_in: number;
};

const createToken = async (endpoint: string, body: TokenRequest) => {
    return withRetry(
        exponentialBackoff(100),
        5,
    )(async () => {
        const response = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams(body),
        });

        if (!response.ok) {
            throw new Error(`Failed to obtain OAuth token: ${await response.text()}`);
        }

        return response.json() as Promise<TokenResponse>;
    });
};
