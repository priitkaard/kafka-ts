export const shared = <F extends (...args: any[]) => Promise<any>>(func: F) => {
    const promises: Record<string, Promise<any>> = {};
    return (...args: Parameters<F>): ReturnType<F> => {
        const key = JSON.stringify(args);
        if (!promises[key]) {
            promises[key] = func(...args);

            const cleanup = () => delete promises[key];
            promises[key].then(cleanup, cleanup);
        }
        return promises[key] as ReturnType<F>;
    };
};
