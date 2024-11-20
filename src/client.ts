import { TcpSocketConnectOpts } from 'net';
import { TLSSocketOptions } from 'tls';
import { SASLProvider } from './broker';
import { Cluster } from './cluster';
import { Consumer, ConsumerOptions } from './consumer/consumer';
import { Producer, ProducerOptions } from './producer/producer';

export type ClientOptions = {
    clientId?: string | null;
    bootstrapServers: TcpSocketConnectOpts[];
    sasl?: SASLProvider | null;
    ssl?: TLSSocketOptions | null;
    requestTimeout?: number;
};

export class Client {
    private options: Required<ClientOptions>;

    constructor(options: ClientOptions) {
        this.options = {
            ...options,
            clientId: options.clientId ?? null,
            sasl: options.sasl ?? null,
            ssl: options.ssl ?? null,
            requestTimeout: options.requestTimeout ?? 60_000,
        };
    }

    public async startConsumer(options: ConsumerOptions) {
        const consumer = new Consumer(this.createCluster(), options);
        await consumer.start();
        return consumer;
    }

    public createProducer(options: ProducerOptions) {
        return new Producer(this.createCluster(), options);
    }

    public createCluster() {
        return new Cluster({
            clientId: this.options.clientId,
            bootstrapServers: this.options.bootstrapServers,
            sasl: this.options.sasl,
            ssl: this.options.ssl,
            requestTimeout: this.options.requestTimeout,
        });
    }
}

export const createKafkaClient = (options: ClientOptions) => new Client(options);
