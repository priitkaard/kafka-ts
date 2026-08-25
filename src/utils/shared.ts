export const shared = <F extends (...args: any[]) => Promise<any>>(func: F) => {
    const promises: Record<string, Promise<any>> = {};
    return (...args: Parameters<F>): ReturnType<F> => {
        const key = JSON.stringify(args);
        if (!promises[key]) {
            const promise = (async () => func(...args))();
            promises[key] = promise;

            const cleanup = () => {
                if (promises[key] === promise) delete promises[key];
            };
            promise.then(cleanup, cleanup);
        }
        return promises[key] as ReturnType<F>;
    };
};
