export class Encoder {
    private buffer: Buffer;
    private offset = 0;

    constructor(initialCapacity = 512) {
        this.buffer = Buffer.allocUnsafe(initialCapacity);
    }

    private ensure(extra: number) {
        const need = this.offset + extra;
        if (need <= this.buffer.length) return;
        let cap = this.buffer.length;
        while (cap < need) cap <<= 1;
        const n = Buffer.allocUnsafe(cap);
        this.buffer.copy(n, 0, 0, this.offset);
        this.buffer = n;
    }

    public getBufferLength() {
        return this.offset;
    }

    public write(src: Buffer) {
        this.ensure(src.length);
        src.copy(this.buffer, this.offset);
        this.offset += src.length;
        return this;
    }

    public writeEncoder(other: Encoder) {
        this.write(other.buffer.subarray(0, other.offset));
        return this;
    }

    public writeInt8(value: number) {
        this.ensure(1);
        this.buffer.writeInt8(value, this.offset);
        this.offset += 1;
        return this;
    }
    public writeInt16(value: number) {
        this.ensure(2);
        this.buffer.writeInt16BE(value, this.offset);
        this.offset += 2;
        return this;
    }
    public writeInt32(value: number) {
        this.ensure(4);
        this.buffer.writeInt32BE(value, this.offset);
        this.offset += 4;
        return this;
    }
    public writeUInt32(value: number) {
        this.ensure(4);
        this.buffer.writeUInt32BE(value, this.offset);
        this.offset += 4;
        return this;
    }
    public writeInt64(value: bigint) {
        this.ensure(8);
        this.buffer.writeBigInt64BE(value, this.offset);
        this.offset += 8;
        return this;
    }

    public writeUVarInt(value: number) {
        const byteArray = [];
        while ((value & 0xffffff80) !== 0) {
            byteArray.push((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        byteArray.push(value & 0x7f);
        this.write(Buffer.from(byteArray));
        return this;
    }
    public writeVarInt(value: number) {
        return this.writeUVarInt((value << 1) ^ (value >> 31));
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
        return this.writeUVarLong((value << 1n) ^ (value >> 63n));
    }

    public writeString(value: string | null) {
        if (value === null) return this.writeInt16(-1);
        const buffer = Buffer.from(value, 'utf-8');
        this.writeInt16(buffer.length);
        this.write(buffer);
        return this;
    }
    public writeCompactString(value: string | null) {
        if (value === null) return this.writeUVarInt(0);
        const b = Buffer.from(value, 'utf-8');
        this.writeUVarInt(b.length + 1);
        this.write(b);
        return this;
    }
    public writeVarIntString(value: string | null) {
        if (value === null) return this.writeVarInt(-1);
        const b = Buffer.from(value, 'utf-8');
        this.writeVarInt(b.length);
        this.write(b);
        return this;
    }
    public writeUUID(value: string | null) {
        if (value === null) {
            this.ensure(16);
            this.buffer.fill(0, this.offset, this.offset + 16);
            this.offset += 16;
            return this;
        }
        this.write(Buffer.from(value, 'hex'));
        return this;
    }
    public writeBoolean(value: boolean) {
        return this.writeInt8(value ? 1 : 0);
    }

    public writeArray<T>(arr: T[], callback: (encoder: Encoder, item: T) => void) {
        this.writeInt32(arr.length);
        for (const it of arr) callback(this, it);
        return this;
    }
    public writeCompactArray<T>(arr: T[] | null, callback: (encoder: Encoder, item: T) => void) {
        if (arr === null) return this.writeUVarInt(0);
        this.writeUVarInt(arr.length + 1);
        for (const it of arr) callback(this, it);
        return this;
    }
    public writeVarIntArray<T>(arr: T[], callback: (encoder: Encoder, item: T) => void) {
        this.writeVarInt(arr.length);
        for (const it of arr) callback(this, it);
        return this;
    }

    public writeBytes(value: Buffer) {
        this.writeInt32(value.length);
        this.write(value);
        return this;
    }
    public writeCompactBytes(value: Buffer) {
        this.writeUVarInt(value.length + 1);
        this.write(value);
        return this;
    }

    public value() {
        return this.buffer.subarray(0, this.offset);
    }
}
