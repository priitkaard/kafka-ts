export const shared = <F extends (...args: any[]) => Promise<any>>(func: F) => {
    let promises: Record<string, Promise<any>> = {};
    return (...args: Parameters<F>): ReturnType<F> => {
        const key = JSON.stringify(args);
        if (!promises[key]) {
            promises[key] = func();
            promises[key].finally(() => {
                delete promises[key];
            });
        }
        return promises[key] as ReturnType<F>;
    };
};
