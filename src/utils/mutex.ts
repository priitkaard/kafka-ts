export const createMutex = () => {
    const queue: (() => void)[] = [];
    let isLocked = false;

    const acquire = () => {
        return new Promise<void>((resolve) => {
            if (!isLocked) {
                isLocked = true;
                return resolve();
            }
            queue.push(resolve);
        });
    };

    const release = () => {
        isLocked = false;
        const next = queue.shift();
        next && next();
    };

    const exclusive = async (fn: () => Promise<void>) => {
        await acquire();
        try {
            await fn();
        } finally {
            release();
        }
    };

    return { exclusive };
};
