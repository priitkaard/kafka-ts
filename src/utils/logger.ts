export interface Logger {
    debug: (message: string, metadata?: unknown) => void;
    info: (message: string, metadata?: unknown) => void;
    warn: (message: string, metadata?: unknown) => void;
    error: (message: string, metadata?: unknown) => void;
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
        console.debug(JSON.stringify({ message, metadata, level: 'debug' }, jsonSerializer));
    }
    info(message: string, metadata?: unknown) {
        console.info(JSON.stringify({ message, metadata, level: 'info' }, jsonSerializer));
    }
    warn(message: string, metadata?: unknown) {
        console.warn(JSON.stringify({ message, metadata, level: 'warning' }, jsonSerializer));
    }
    error(message: string, metadata?: unknown) {
        console.error(JSON.stringify({ message, metadata, level: 'error' }, jsonSerializer));
    }
}

export let log: Logger = new JsonLogger();

export const setLogger = (newLogger: Logger) => {
    log = newLogger;
};
