import { API } from '../api';
import { SASLProvider } from '../broker';
import { getErrorMessage } from '../utils/error';
import { log } from '../utils/logger';
import { clamp } from '../utils/number';
import { exponentialBackoff, withRetry } from '../utils/retry';

const MAX_INT = Math.pow(2, 31) - 1;
const RETRY_DELAY_MS = 1_000;
const MAX_RETRY_DELAY_MS = 30_000;
const DEFAULT_EXPIRES_IN_SECONDS = 3_600;

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
    const requestToken = (refreshToken?: string) =>
        createToken(
            endpoint,
            refreshToken
                ? {
                      grant_type: 'refresh_token',
                      client_id: clientId,
                      client_secret: clientSecret,
                      refresh_token: refreshToken,
                  }
                : { grant_type: 'client_credentials', client_id: clientId, client_secret: clientSecret },
        );

    let tokenPromise = requestToken();
    let refreshToken: string | undefined;
    let validUntil = 0;
    let consecutiveFailures = 0;

    const getRetryDelayMs = () => Math.min(RETRY_DELAY_MS * 2 ** consecutiveFailures++, MAX_RETRY_DELAY_MS);

    const scheduleRefresh = (delayMs: number) => setTimeout(() => void refresh(), delayMs).unref();

    const accept = (token: TokenResponse) => {
        const lifetimeSeconds = Math.max(Number(token.expires_in), 0) || DEFAULT_EXPIRES_IN_SECONDS;
        consecutiveFailures = 0;

        refreshToken = token.refresh_token;
        validUntil = Date.now() + lifetimeSeconds * 1000;
        scheduleRefresh(getRefreshInMs(lifetimeSeconds, refreshThresholdSeconds));
    };

    const refresh = async () => {
        const pending = requestToken(refreshToken);
        if (Date.now() >= validUntil) tokenPromise = pending;

        try {
            const token = await pending;
            tokenPromise = pending;
            accept(token);
        } catch (error) {
            log.warn('Failed to refresh the OAuth token. Retrying...', { reason: getErrorMessage(error) });

            refreshToken = undefined;
            scheduleRefresh(getRetryDelayMs());
        }
    };

    tokenPromise.then(accept, (error) => {
        log.warn('Failed to obtain OAuth token. Retrying...', { reason: getErrorMessage(error) });
        scheduleRefresh(getRetryDelayMs());
    });

    return () => tokenPromise;
};

const getRefreshInMs = (lifetimeSeconds: number, refreshThresholdSeconds: number) => {
    const aheadOfExpiry = lifetimeSeconds - refreshThresholdSeconds;
    const refreshInSeconds = aheadOfExpiry > 0 ? aheadOfExpiry : lifetimeSeconds / 2;

    return clamp(refreshInSeconds * 1000, RETRY_DELAY_MS, MAX_INT);
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
