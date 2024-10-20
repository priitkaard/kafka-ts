import { serializer } from "./debug";

export const trace =
    (fn?: (...args: any[]) => Record<string, unknown> | undefined) =>
    (target: any, propertyKey: string, descriptor: PropertyDescriptor) => {
        if (!process.env.DEBUG?.includes("kafka-ts")) return;

        const original = descriptor.value;
        descriptor.value = function (...args: any[]) {
            const startTime = Date.now();
            const metadata = fn?.(...args);

            const onEnd = <T>(result: T): T => {
                console.log(
                    `[${propertyKey}] +${Date.now() - startTime}ms ${JSON.stringify({ ...metadata, result }, serializer)}`,
                );
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
