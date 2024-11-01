import { delay } from './delay';
import { log } from './logger';

export type Retrier = (func: () => unknown) => Promise<void>;

export const createExponentialBackoffRetrier =
    ({
        retries = 5,
        initialDelayMs = 100,
        maxDelayMs = 3000,
        multiplier = 2,
        onFailure = (error) => {
            throw error;
        },
    }: {
        retries?: number;
        initialDelayMs?: number;
        maxDelayMs?: number;
        multiplier?: number;
        onFailure?: (error: unknown) => unknown;
    } = {}): Retrier =>
    async (func): Promise<void> => {
        let retriesLeft = retries;
        let delayMs = initialDelayMs;
        let lastError: unknown | undefined;

        while (true) {
            try {
                await func();
                return;
            } catch (error) {
                lastError = error;
            }
            log.debug(`Failed to process batch (retriesLeft: ${retriesLeft})`);

            if (--retriesLeft < 1) break;

            await delay(delayMs);
            delayMs = Math.min(maxDelayMs, delayMs * multiplier);
        }

        await onFailure(lastError);
    };

export const defaultRetrier = createExponentialBackoffRetrier();
