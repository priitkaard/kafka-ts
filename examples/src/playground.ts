import { EventEmitter } from 'stream';
import { kafka } from './client';

const producer = kafka.createProducer({ allowTopicAutoCreation: true, maxBatchSize: 500 });

const CONCURRENCY = parseInt(process.env.CONCURRENCY ?? '5000');

let counter = 0;
let pendingRequests = 0;

setInterval(() => {
    console.log(`Pending requests: ${pendingRequests}`);

    console.log(`Message rate: ${counter} msg/s`);
    counter = 0;
}, 1000);

const emitter = new EventEmitter();

(async () => {
    while (true) {
        if (pendingRequests >= CONCURRENCY) {
            await new Promise((resolve) => emitter.once('drain', resolve));
            continue;
        }
    
        void (async () => {
            pendingRequests++;
            await producer.send([{ topic: 'my-topic', value: 'ping' }]);
            pendingRequests--;
            counter++;
            if (pendingRequests < CONCURRENCY) emitter.emit('drain');
        })();
    }
})();
