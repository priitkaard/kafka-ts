import EventEmitter from 'events';
import { log } from './logger';

export class Lock extends EventEmitter {
    private locks: Record<string, boolean> = {};

    constructor() {
        super();
        this.setMaxListeners(Infinity);
    }

    public async acquire(keys: string[], callback: () => Promise<void>) {
        await Promise.all(keys.map((key) => this.acquireKey(key)));
        try {
            await callback();
        } finally {
            keys.forEach((key) => this.releaseKey(key));
        }
    }

    private async acquireKey(key: string) {
        while (this.locks[key]) {
            await new Promise<void>((resolve) => {
                const timeout = setTimeout(() => {
                    log.warn(`Lock timed out`, { key });
                    this.releaseKey(key);
                }, 60_000);

                this.once(`release:${key}`, () => {
                    clearTimeout(timeout);
                    resolve();
                });
            });
        }
        this.locks[key] = true;
    }

    private releaseKey(key: string) {
        this.locks[key] = false;
        this.emit(`release:${key}`);
    }
}
