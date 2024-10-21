export const serializer = (_: string, value: unknown) => (typeof value === 'bigint' ? value.toString() : value);
