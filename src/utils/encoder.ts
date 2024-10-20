export class Encoder {
    private buffer: Buffer;

    constructor({ buffer = Buffer.alloc(0) }: { buffer?: Buffer } = {}) {
        this.buffer = buffer;
    }

    public write(rightBuffer: Buffer) {
        this.buffer = Buffer.concat([this.buffer, rightBuffer]);
        return this;
    }

    public writeInt8(value: number) {
        const buffer = Buffer.alloc(1);
        buffer.writeInt8(value);
        return this.write(buffer);
    }

    public writeInt16(value: number) {
        const buffer = Buffer.alloc(2);
        buffer.writeInt16BE(value);
        return this.write(buffer);
    }

    public writeInt32(value: number) {
        const buffer = Buffer.alloc(4);
        buffer.writeInt32BE(value);
        return this.write(buffer);
    }

    public writeUInt32(value: number) {
        const buffer = Buffer.alloc(4);
        buffer.writeUInt32BE(value);
        return this.write(buffer);
    }

    public writeInt64(value: bigint) {
        const buffer = Buffer.alloc(8);
        buffer.writeBigInt64BE(value);
        return this.write(buffer);
    }

    public writeUVarInt(value: number) {
        const byteArray = [];
        while ((value & 0xffffffff) !== 0) {
            byteArray.push((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        byteArray.push(value & 0x7f);
        return this.write(Buffer.from(byteArray));
    }

    public writeVarInt(value: number) {
        const encodedValue = (value << 1) ^ (value >> 31);
        return this.writeUVarInt(encodedValue);
    }

    public writeUVarLong(value: bigint) {
        const byteArray = [];
        while ((value & BigInt(0xffffffffffffffff)) !== BigInt(0)) {
            byteArray.push(Number((value & BigInt(0x7f)) | BigInt(0x80)));
            value = value >> BigInt(7);
        }
        byteArray.push(Number(value));
        return this.write(Buffer.from(byteArray));
    }

    public writeVarLong(value: bigint) {
        const encodedValue = (value << BigInt(1)) ^ (value >> BigInt(63));
        return this.writeUVarLong(encodedValue);
    }

    public writeString(value: string | null) {
        if (value === null) {
            return this.writeInt16(-1);
        }
        const byteLength = Buffer.byteLength(value, "utf-8");
        const buffer = Buffer.alloc(byteLength);
        buffer.write(value, 0, byteLength, "utf-8");
        return this.writeInt16(byteLength).write(buffer);
    }

    public writeCompactString(value: string | null) {
        if (value === null) {
            return this.writeUVarInt(0);
        }

        const byteLength = Buffer.byteLength(value, "utf-8");
        const buffer = Buffer.alloc(byteLength);
        buffer.write(value, 0, byteLength, "utf-8");
        return this.writeUVarInt(byteLength + 1).write(buffer);
    }

    public writeVarIntString(value: string | null) {
        if (value === null) {
            return this.writeVarInt(-1);
        }
        return this.writeVarInt(Buffer.byteLength(value, "utf-8")).write(Buffer.from(value, "utf-8"));
    }

    public writeUUID(value: string | null) {
        if (value === null) {
            return this.write(Buffer.alloc(16));
        }
        return this.write(Buffer.from(value, "hex"));
    }

    public writeBoolean(value: boolean) {
        return this.writeInt8(value ? 1 : 0);
    }

    public writeArray<T>(arr: T[], callback: (encoder: Encoder, item: T) => Encoder) {
        const buffers = arr.map((item) => callback(new Encoder(), item).value());
        return this.writeInt32(arr.length).write(Buffer.concat(buffers));
    }

    public writeCompactArray<T>(arr: T[] | null, callback: (encoder: Encoder, item: T) => Encoder) {
        if (arr === null) {
            return this.writeUVarInt(0);
        }
        const buffers = arr.map((item) => callback(new Encoder(), item).value());
        return this.writeUVarInt(buffers.length + 1).write(Buffer.concat(buffers));
    }

    public writeVarIntArray<T>(arr: T[], callback: (encoder: Encoder, item: T) => Encoder) {
        const buffers = arr.map((item) => callback(new Encoder(), item).value());
        return this.writeVarInt(buffers.length).write(Buffer.concat(buffers));
    }

    public writeBytes(value: Buffer) {
        return this.writeInt32(value.length).write(value);
    }

    public writeCompactBytes(value: Buffer) {
        return this.writeUVarInt(value.length + 1).write(value);
    }

    public value() {
        return this.buffer;
    }
}
