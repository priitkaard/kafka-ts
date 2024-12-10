export const shared = <F extends () => Promise<any>>(func: F) => {
    let promise: Promise<any> | undefined;
    return (): ReturnType<F> => {
        if (!promise) {
            promise = func();
            promise.finally(() => {
                promise = undefined;
            });
        }
        return promise as ReturnType<F>;
    };
};
