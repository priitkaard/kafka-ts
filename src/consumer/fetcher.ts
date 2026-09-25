import { FetchResponse } from '../api/fetch';
import { Assignment } from '../api/sync-group';
import { createTracer } from '../utils/tracer';

const trace = createTracer('Fetcher');

type FetcherOptions = {
    nodeId: number;
    assignment: Assignment;
    fetch: (nodeId: number, assignment: Assignment, previous?: FetchResponse) => Promise<FetchResponse>;
    process: (response: FetchResponse) => Promise<void>;
};

export class Fetcher {
    private isRunning = false;
    private cancelFetch = () => {};
    private stopped = Promise.resolve();

    constructor(private options: FetcherOptions) {}

    public loop() {
        this.isRunning = true;
        this.stopped = this.run();
        return this.stopped;
    }

    private async run() {
        let response = await this.fetch();
        while (this.isRunning && response) {
            const nextResponse = this.fetch(response);
            nextResponse.catch(() => {});
            await this.process(response);
            response = await nextResponse;
        }
    }

    private fetch(previous?: FetchResponse) {
        const { nodeId, assignment, fetch } = this.options;

        return new Promise<FetchResponse | undefined>((resolve, reject) => {
            this.cancelFetch = () => resolve(undefined);
            fetch(nodeId, assignment, previous).then(resolve, reject);
        });
    }

    @trace()
    private async process(response: FetchResponse) {
        await this.options.process(response);
    }

    public async stop() {
        this.isRunning = false;
        this.cancelFetch();
        await this.stopped.catch(() => {});
    }
}
