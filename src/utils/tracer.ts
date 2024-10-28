import { log } from './logger';

export interface Tracer {
    startActiveSpan<T>(module: string, method: string, metadata: Record<string, unknown>, callback: () => T): T;
}

class DebugTracer implements Tracer {
    private isEnabled = process.env.DEBUG?.includes('kafka-ts');

    startActiveSpan<T>(module: string, method: string, metadata: Record<string, unknown>, callback: () => T): T {
        if (!this.isEnabled) {
            return callback();
        }

        const startTime = Date.now();

        const onEnd = <T>(result: T): T => {
            log.debug(`[${module}.${method}] ${metadata?.message ?? ''} +${Date.now() - startTime}ms`, {
                ...metadata,
                ...(!!result && { result }),
            });
            return result;
        };

        const result = callback();
        if (result instanceof Promise) {
            return result.then(onEnd) as T;
        }
        onEnd(result);
        return result;
    }
}

let tracer: Tracer = new DebugTracer();

export const setTracer = <T>(newTracer: Tracer) => {
    tracer = newTracer;
};

export const createTracer =
    (module: string) =>
    (fn?: (...args: any[]) => Record<string, unknown> | undefined) =>
    (target: any, propertyKey: string, descriptor: PropertyDescriptor) => {
        const original = descriptor.value;
        descriptor.value = function (...args: any[]) {
            const metadata = fn?.(...args);
            return tracer.startActiveSpan(module, propertyKey, { ...metadata }, () => original.apply(this, args));
        };
    };

export const trace = createTracer('GLOBAL');
