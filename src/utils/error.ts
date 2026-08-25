import { API_ERROR } from '../api';

export class KafkaTSError extends Error {
    constructor(message: string) {
        super(message);
        this.name = this.constructor.name;
    }
}

export class KafkaTSApiError<T = any> extends KafkaTSError {
    public apiName: string | undefined;
    public request: unknown | undefined;

    constructor(
        public errorCode: number,
        public errorMessage: string | null,
        public response: T,
    ) {
        const [errorName] = Object.entries(API_ERROR).find(([, value]) => value === errorCode) ?? ['UNKNOWN'];
        super(`${errorName}${errorMessage ? `: ${errorMessage}` : ''}`);
    }
}

export class ConnectionError extends KafkaTSError {
    constructor(message: string, stack?: string) {
        super(message);
        this.stack += `\n${stack}`;
    }
}

export const getErrorMessage = (error: unknown): string => {
    if (!(error instanceof Error)) return String(error);

    if (error instanceof AggregateError) {
        const messages = (error.errors as unknown[])
            .map((cause) => (cause instanceof Error ? getErrorMessage(cause) : String(cause)))
            .filter(Boolean);
        if (messages.length) return messages.join(', ');
    }

    return error.message || (error as NodeJS.ErrnoException).code || error.name;
};
