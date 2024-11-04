export class Encoder {
    private chunks: Buffer[] = [];

    public getChunks() {
        return this.chunks;
    }

    public getBufferLength() {
        return this.chunks.reduce((acc, chunk) => acc + chunk.length, 0);
    }

    public write(...buffers: Buffer[]) {
        this.chunks.push(...buffers);
        return this;
    }

    public writeEncoder(encoder: Encoder) {
        return this.write(...encoder.getChunks());
    }

    public writeInt8(value: number) {
        const buffer = Buffer.allocUnsafe(1);
        buffer.writeInt8(value);
        return this.write(buffer);
    }

    public writeInt16(value: number) {
        const buffer = Buffer.allocUnsafe(2);
        buffer.writeInt16BE(value);
        return this.write(buffer);
    }

    public writeInt32(value: number) {
        const buffer = Buffer.allocUnsafe(4);
        buffer.writeInt32BE(value);
        return this.write(buffer);
    }

    public writeUInt32(value: number) {
        const buffer = Buffer.allocUnsafe(4);
        buffer.writeUInt32BE(value);
        return this.write(buffer);
    }

    public writeInt64(value: bigint) {
        const buffer = Buffer.allocUnsafe(8);
        buffer.writeBigInt64BE(value);
        return this.write(buffer);
    }

    public writeUVarInt(value: number) {
        const byteArray = [];
        while ((value & 0xffffff80) !== 0) {
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
        while ((value & 0xffffffffffffff80n) !== 0n) {
            byteArray.push(Number((value & BigInt(0x7f)) | BigInt(0x80)));
            value >>= 7n;
        }
        byteArray.push(Number(value & BigInt(0x7f)));
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
        const buffer = Buffer.from(value, 'utf-8');
        return this.writeInt16(buffer.length).write(buffer);
    }

    public writeCompactString(value: string | null) {
        if (value === null) {
            return this.writeUVarInt(0);
        }

        const buffer = Buffer.from(value, 'utf-8');
        return this.writeUVarInt(buffer.length + 1).write(buffer);
    }

    public writeVarIntString(value: string | null) {
        if (value === null) {
            return this.writeVarInt(-1);
        }
        const buffer = Buffer.from(value, 'utf-8');
        return this.writeVarInt(buffer.length).write(buffer);
    }

    public writeUUID(value: string | null) {
        if (value === null) {
            return this.write(Buffer.alloc(16));
        }
        return this.write(Buffer.from(value, 'hex'));
    }

    public writeBoolean(value: boolean) {
        return this.writeInt8(value ? 1 : 0);
    }

    public writeArray<T>(arr: T[], callback: (encoder: Encoder, item: T) => Encoder) {
        return this.writeInt32(arr.length).write(...arr.flatMap((item) => callback(new Encoder(), item).getChunks()));
    }

    public writeCompactArray<T>(arr: T[] | null, callback: (encoder: Encoder, item: T) => Encoder) {
        if (arr === null) {
            return this.writeUVarInt(0);
        }
        return this.writeUVarInt(arr.length + 1).write(
            ...arr.flatMap((item) => callback(new Encoder(), item).getChunks()),
        );
    }

    public writeVarIntArray<T>(arr: T[], callback: (encoder: Encoder, item: T) => Encoder) {
        return this.writeVarInt(arr.length).write(...arr.flatMap((item) => callback(new Encoder(), item).getChunks()));
    }

    public writeBytes(value: Buffer) {
        return this.writeInt32(value.length).write(value);
    }

    public writeCompactBytes(value: Buffer) {
        return this.writeUVarInt(value.length + 1).write(value);
    }

    public value() {
        return Buffer.concat(this.chunks);
    }
}
