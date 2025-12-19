import { delay } from './delay';
import { log } from './logger';

export const withRetry =
    (handleError: (error: unknown, retry: number) => Promise<void>, maxRetries = 15) =>
    async <T>(func: () => Promise<T>): Promise<T> => {
        let lastError: unknown | undefined;
        for (let i = 0; i < maxRetries; i++) {
            try {
                return await func();
            } catch (error) {
                await handleError(error, i + 1);
                lastError = error;
            }
        }
        log.warn('Retries exhausted', { lastError });
        throw lastError;
    };

export const exponentialBackoff =
    (initialDelayMs: number, maxDelayMs = 5_000) =>
    async (_: unknown, retry: number) => {
        const delayMs = Math.min(maxDelayMs, initialDelayMs * 2 ** retry);
        await delay(delayMs);
    };
