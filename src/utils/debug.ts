export const serializer = (_: string, value: unknown) => (typeof value === "bigint" ? value.toString() : value);

export const createDebugger = (module: string) => (func: string, message: string, data?: unknown) => {
    if (!process.env.DEBUG?.includes("kafka-ts")) return;
    console.debug(
        `[${module}] ${func}: ${message}`,
        data && `(${data instanceof Error ? data : JSON.stringify(data, serializer, 4)})`,
    );
};
