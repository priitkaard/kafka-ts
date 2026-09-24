export class PromiseChain {
    private locks = new Map<string, Promise<void>>();

    public async run(keys: string[], callback: () => Promise<void>) {
        const orderedKeys = [...new Set(keys)].sort();
        const releases: (() => void)[] = [];

        for (const key of orderedKeys) {
            const release = await this.acquire(key);
            releases.push(release);
        }

        try {
            await callback();
        } finally {
            releases.reverse().forEach((release) => release());
        }
    }

    private async acquire(key: string): Promise<() => void> {
        const previousTail = this.locks.get(key);

        let release: () => void;
        const currentTail = new Promise<void>((resolve) => (release = resolve));
        const tail = previousTail ? previousTail.then(() => currentTail) : currentTail;

        this.locks.set(key, tail);
        if (previousTail) await previousTail;

        return () => {
            release();
            if (this.locks.get(key) === tail) {
                this.locks.delete(key);
            }
        };
    }
}
