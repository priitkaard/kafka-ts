/* https://github.com/apache/kafka/blob/0.10.2/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L364 */

export const murmur2 = (data: Buffer): number => {
    const length = data.length;
    const seed = 0x9747b28c;

    const m = 0x5bd1e995;
    const r = 24;

    let h = seed ^ length;
    let length4 = Math.floor(length / 4);

    for (let i = 0; i < length4; i++) {
        const i4 = i * 4;
        let k =
            (data[i4 + 0] & 0xff) +
            ((data[i4 + 1] & 0xff) << 8) +
            ((data[i4 + 2] & 0xff) << 16) +
            ((data[i4 + 3] & 0xff) << 24);
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
    }

    switch (length % 4) {
        case 3:
            h = h ^ ((data[(length & ~3) + 2] & 0xff) << 16);
        case 2:
            h = h ^ ((data[(length & ~3) + 1] & 0xff) << 8);
        case 1:
            h = h ^ (data[length & ~3] & 0xff);
            h *= m;
    }

    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return h;
};

export const toPositive = (input: number) => input & 0x7fffffff;
