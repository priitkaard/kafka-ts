export const cached = <F extends (...args: any[]) => any>(func: F, createKey: (...args: Parameters<F>) => string) => {
    const cache: Record<string, any> = {};

    const cachedFunc = ((...args: Parameters<F>): ReturnType<F> => {
        const key = createKey(...args);
        if (!(key in cache)) {
            cache[key] = func(...args);
        }
        return cache[key] as ReturnType<F>;
    }) as F & { clear: () => void };

    cachedFunc.clear = () => {
        for (const key in cache) delete cache[key];
    };

    return cachedFunc;
};
