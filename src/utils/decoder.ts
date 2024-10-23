export class Decoder {
    private offset = 0;

    constructor(private buffer: Buffer) {}

    public getOffset() {
        return this.offset;
    }

    public getBuffer() {
        return this.buffer;
    }

    public getBufferLength() {
        return this.buffer.length;
    }

    public readInt8() {
        const value = this.buffer.readInt8(this.offset);
        this.offset += 1;
        return value;
    }

    public readInt16() {
        const value = this.buffer.readInt16BE(this.offset);
        this.offset += 2;
        return value;
    }

    public readInt32() {
        const value = this.buffer.readInt32BE(this.offset);
        this.offset += 4;
        return value;
    }

    public readUInt32() {
        const value = this.buffer.readUInt32BE(this.offset);
        this.offset += 4;
        return value;
    }

    public readInt64() {
        const value = this.buffer.readBigInt64BE(this.offset);
        this.offset += 8;
        return value;
    }

    public readUVarInt() {
        let result = 0;
        let shift = 0;
        let currentByte;
        do {
            currentByte = this.buffer[this.offset++];
            result |= (currentByte & 0x7f) << shift;
            shift += 7;
        } while ((currentByte & 0x80) !== 0);
        return result;
    }

    public readVarInt() {
        const decodedValue = this.readUVarInt();
        return (decodedValue >>> 1) ^ -(decodedValue & 1);
    }

    public readUVarLong() {
        let result = BigInt(0);
        let shift = BigInt(0);
        let currentByte;
        do {
            currentByte = BigInt(this.buffer[this.offset++]);
            result |= (currentByte & BigInt(0x7f)) << shift;
            shift += BigInt(7);
        } while ((currentByte & BigInt(0x80)) !== BigInt(0));
        return result;
    }

    public readVarLong() {
        const decodedValue = this.readUVarLong();
        return (decodedValue >> BigInt(1)) ^ -(decodedValue & BigInt(1));
    }

    public readString() {
        const length = this.readInt16();
        if (length < 0) {
            return null;
        }

        const value = this.buffer.toString('utf-8', this.offset, this.offset + length);
        this.offset += length;
        return value;
    }

    public readCompactString() {
        const length = this.readUVarInt() - 1;
        if (length < 0) {
            return null;
        }

        const value = this.buffer.toString('utf-8', this.offset, this.offset + length);
        this.offset += length;
        return value;
    }

    public readVarIntBuffer() {
        const length = this.readVarInt();
        if (length < 0) {
            return null;
        }

        const value = this.buffer.subarray(this.offset, this.offset + length);
        this.offset += length;
        return value;
    }

    public readUUID() {
        const value = this.buffer.toString('hex', this.offset, this.offset + 16);
        this.offset += 16;
        return value;
    }

    public readBoolean() {
        const value = this.buffer.readInt8(this.offset) === 1;
        this.offset += 1;
        return value;
    }

    public readArray<T>(callback: (opts: Decoder) => T): T[] {
        const length = this.readInt32();
        const results = Array.from({ length }).map(() => callback(this));
        return results;
    }

    public readCompactArray<T>(callback: (opts: Decoder) => T): T[] {
        const length = this.readUVarInt() - 1;
        const results = Array.from({ length }).map(() => callback(this));
        return results;
    }

    public readVarIntArray<T>(callback: (opts: Decoder) => T): T[] {
        const length = this.readVarInt();
        const results = Array.from({ length }).map(() => callback(this));
        return results;
    }

    public readRecords<T>(callback: (opts: Decoder) => T): T[] {
        const length = this.readInt32();

        return Array.from({ length }).map(() => {
            const size = this.readVarInt();
            const child = new Decoder(this.buffer.subarray(this.offset, this.offset + size));
            this.offset += size;
            return callback(child);
        });
    }

    public read(length: number) {
        const value = this.buffer.subarray(this.offset, this.offset + length);
        this.offset += length;
        return value;
    }

    public readBytes() {
        const length = this.readInt32();
        return this.read(length);
    }

    public readCompactBytes() {
        const length = this.readUVarInt() - 1;
        if (length < 0) {
            return null;
        }
        return this.read(length);
    }

    public readTagBuffer() {
        this.readUVarInt();
    }
}
