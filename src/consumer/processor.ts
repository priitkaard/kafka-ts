import { EventEmitter } from 'stream';
import { Batch } from '../types';
import { createTracer } from '../utils/tracer';

const trace = createTracer('Processor');

type ProcessorOptions = {
    poll: () => Promise<Batch>;
    process: (batch: Batch) => Promise<void>;
};

export class Processor extends EventEmitter<{ stop: []; stopped: [] }> {
    private isRunning = false;

    constructor(private options: ProcessorOptions) {
        super();
    }

    public async loop() {
        const { poll, process } = this.options;

        this.isRunning = true;
        this.once('stop', () => (this.isRunning = false));

        try {
            while (this.isRunning) {
                const batch = await poll();
                if (batch.length) {
                    await process(batch);
                }
            }
        } finally {
            this.isRunning = false;
            this.emit('stopped');
        }
    }

    @trace()
    public async stop() {
        if (!this.isRunning) {
            return;
        }

        return new Promise<void>((resolve) => {
            this.once('stopped', resolve);
            this.emit('stop');
        });
    }
}
