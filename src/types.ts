export type Message = {
    topic: string;
    partition: number;
    offset?: bigint;
    timestamp?: bigint;
    key: string | null;
    value: string | null;
    headers?: Record<string, string>;
};
