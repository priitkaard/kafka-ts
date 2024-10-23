export type Message = {
    topic: string;
    partition: number;
    offset?: bigint;
    timestamp?: bigint;
    key: Buffer | null;
    value: Buffer | null;
    headers?: Record<string, string>;
};

export type Batch = Required<Message>[];
