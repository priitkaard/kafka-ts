import { ConnectionError } from './error';

export const withTimeout = <T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> =>
    new Promise<T>((resolve, reject) => {
        const timeout = setTimeout(() => reject(new ConnectionError(message)), timeoutMs);
        promise.then(resolve, reject).finally(() => clearTimeout(timeout));
    });
