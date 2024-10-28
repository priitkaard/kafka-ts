import { EventEmitter } from 'stream';
import { Batch } from '../types';
import { createTracer } from '../utils/tracer';

const trace = createTracer('Processor');

type ProcessorOptions = {
    poll: () => Promise<Batch>;
    process: (batch: Batch) => Promise<void>;
};

export class Processor extends EventEmitter<{ stopped: [] }> {
    private isRunning = false;

    constructor(private options: ProcessorOptions) {
        super();
    }

    public async loop() {
        this.isRunning = true;

        try {
            while (this.isRunning) {
                await this.step();
            }
        } finally {
            this.isRunning = false;
            this.emit('stopped');
        }
    }

    @trace()
    private async step() {
        const { poll, process } = this.options;

        const batch = await poll();
        if (batch.length) {
            await process(batch);
        }
    }

    public async stop() {
        if (!this.isRunning) {
            return;
        }

        const stopPromise = new Promise<void>((resolve) => {
            this.once('stopped', resolve);
        });
        this.isRunning = false;
        return stopPromise;
    }
}
