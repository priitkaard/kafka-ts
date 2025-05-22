export interface Logger {
    debug: (message: string, metadata?: unknown) => void;
    info: (message: string, metadata?: unknown) => void;
    warn: (message: string, metadata?: unknown) => void;
    error: (message: string, metadata?: unknown) => void;
}

export enum LogLevel {
    DEBUG,
    INFO,
    WARNING,
    ERROR,
}

export const jsonSerializer = (_: unknown, v: unknown) => {
    if (v instanceof Error) {
        return Object.getOwnPropertyNames(v).reduce(
            (acc, key) => {
                acc[key] = v[key as keyof Error];
                return acc;
            },
            {} as Record<string, unknown>,
        );
    }
    if (Buffer.isBuffer(v)) {
        return v.toString();
    }
    if (typeof v === 'bigint') {
        return v.toString();
    }
    return v;
};

class JsonLogger implements Logger {
    debug(message: string, metadata?: unknown) {
        logLevel <= LogLevel.DEBUG &&
            console.debug(JSON.stringify({ message, metadata, level: 'debug' }, jsonSerializer));
    }
    info(message: string, metadata?: unknown) {
        logLevel <= LogLevel.INFO && console.info(JSON.stringify({ message, metadata, level: 'info' }, jsonSerializer));
    }
    warn(message: string, metadata?: unknown) {
        logLevel <= LogLevel.WARNING &&
            console.warn(JSON.stringify({ message, metadata, level: 'warning' }, jsonSerializer));
    }
    error(message: string, metadata?: unknown) {
        logLevel <= LogLevel.ERROR &&
            console.error(JSON.stringify({ message, metadata, level: 'error' }, jsonSerializer));
    }
}

export let log: Logger = new JsonLogger();
export const setLogger = (newLogger: Logger) => {
    log = newLogger;
};

let logLevel: LogLevel = LogLevel.INFO;
export const setLogLevel = (newLogLevel: LogLevel) => {
    logLevel = newLogLevel;
};
