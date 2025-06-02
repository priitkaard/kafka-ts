import { log } from './logger';

export const withRetry =
    (handleError: (error: unknown) => Promise<void>) =>
    async <T>(func: () => Promise<T>): Promise<T> => {
        let lastError: unknown | undefined;
        for (let i = 0; i < 15; i++) {
            try {
                return await func();
            } catch (error) {
                await handleError(error);
                lastError = error;
            }
        }
        log.warn('Retries exhausted', { lastError });
        throw lastError;
    };
