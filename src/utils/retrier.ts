import { delay } from "./delay";

export type Retrier = (func: () => unknown) => Promise<void>;

export const createExponentialBackoffRetrier =
    (options: {
        onFailure?: (error: unknown) => Promise<void>;
        maxRetries?: number;
        initialDelayMs?: number;
        maxDelayMs?: number;
        multiplier?: number;
        retry?: number;
    }): Retrier =>
    async (func) => {
        try {
            await func();
        } catch (error) {
            const {
                retry = 0,
                maxRetries = 3,
                onFailure = (error) => {
                    throw error;
                },
                initialDelayMs = 100,
                maxDelayMs = 3000,
                multiplier = 2,
            } = options;

            const isMaxRetriesExceeded = retry > maxRetries;
            if (isMaxRetriesExceeded) return onFailure(error);

            const delayMs = Math.min(maxDelayMs, initialDelayMs * multiplier ** retry);
            await delay(delayMs);

            return createExponentialBackoffRetrier({ ...options, retry: retry + 1 })(func);
        }
    };

export const defaultRetrier = createExponentialBackoffRetrier({});
