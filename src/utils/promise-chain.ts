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

        if (previousTail) {
            this.locks.set(
                key,
                previousTail.then(() => currentTail),
            );
            await previousTail;
        } else {
            this.locks.set(key, currentTail);
        }

        return () => {
            release();
            if (this.locks.get(key) === currentTail) {
                this.locks.delete(key);
            }
        };
    }
}
