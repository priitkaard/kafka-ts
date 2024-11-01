import { API_ERROR } from '../api';

export class KafkaTSError extends Error {
    constructor(message: string) {
        super(message);
        this.name = this.constructor.name;
    }
}

export class KafkaTSApiError<T = any> extends KafkaTSError {
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

export class ConnectionError extends KafkaTSError {}
