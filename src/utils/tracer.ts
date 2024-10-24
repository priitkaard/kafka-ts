import { log } from './logger';

export const createTracer =
    (module: string, attributes?: Record<string, unknown>) =>
    (fn?: (...args: any[]) => Record<string, unknown> | undefined) =>
    (target: any, propertyKey: string, descriptor: PropertyDescriptor) => {
        if (!process.env.DEBUG?.includes('kafka-ts')) return;

        const original = descriptor.value;
        descriptor.value = function (...args: any[]) {
            const startTime = Date.now();
            const metadata = fn?.(...args);

            const onEnd = <T>(result: T): T => {
                log.debug(`[${module}.${propertyKey}] ${metadata?.message ?? ''} +${Date.now() - startTime}ms`, {
                    ...attributes,
                    ...metadata,
                    ...result && { result},
                });
                return result;
            };

            const result = original.apply(this, args);
            if (result instanceof Promise) {
                return result.then(onEnd);
            } else {
                onEnd(result);
                return result;
            }
        };
    };

export const trace = createTracer('GLOBAL');
