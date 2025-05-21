import { TcpSocketConnectOpts } from 'net';
import { TLSSocketOptions } from 'tls';
import { API } from './api';
import { Metadata } from './api/metadata';
import { Broker, SASLProvider } from './broker';
import { SendRequest } from './connection';
import { BrokerNotAvailableError, KafkaTSError } from './utils/error';
import { log } from './utils/logger';
import { createTracer } from './utils/tracer';

const trace = createTracer('Cluster');

type ClusterOptions = {
    clientId: string | null;
    bootstrapServers: TcpSocketConnectOpts[];
    sasl: SASLProvider | null;
    ssl: TLSSocketOptions | null;
    requestTimeout: number;
};

export class Cluster {
    private seedBroker: Broker | undefined;
    private brokerById: Record<number, Broker> = {};
    private brokerMetadata: Record<number, Metadata['brokers'][number]> = {};

    constructor(private options: ClusterOptions) {}

    public async connect() {
        this.seedBroker = await this.findSeedBroker();
        this.brokerById = {};

        await this.refreshBrokerMetadata();
    }
    
    public async refreshBrokerMetadata() {
        const metadata = await this.sendRequest(API.METADATA, { topics: [] });
        this.brokerMetadata = Object.fromEntries(metadata.brokers.map((options) => [options.nodeId, options]));
    }

    public async ensureConnected() {
        if (!this.seedBroker) {
            return this.connect();
        }
        try {
            await Promise.all([this.seedBroker, ...Object.values(this.brokerById)].map((x) => x.ensureConnected()));
        } catch {
            log.warn('Failed to connect to known brokers, reconnecting...');
            await this.disconnect();
            return this.connect();
        }
    }

    public async disconnect() {
        await Promise.all([
            this.seedBroker?.disconnect(),
            ...Object.values(this.brokerById).map((x) => x.disconnect()),
        ]);
    }

    public setSeedBroker = async (nodeId: number) => {
        await this.seedBroker?.disconnect();
        this.seedBroker = await this.acquireBroker(nodeId);
    };

    public sendRequest: SendRequest = (...args) => this.seedBroker!.sendRequest(...args);

    public sendRequestToNode =
        (nodeId: number): SendRequest =>
        async (...args) => {
            if (!this.brokerById[nodeId]) {
                this.brokerById[nodeId] = await this.acquireBroker(nodeId);
            }
            return this.brokerById[nodeId].sendRequest(...args);
        };

    @trace((nodeId) => ({ nodeId, result: `<Broker ${nodeId}>` }))
    public async acquireBroker(nodeId: number) {
        if (!(nodeId in this.brokerMetadata)) {
            throw new BrokerNotAvailableError(nodeId);
        }
        const broker = new Broker({
            clientId: this.options.clientId,
            sasl: this.options.sasl,
            ssl: this.options.ssl,
            requestTimeout: this.options.requestTimeout,
            options: this.brokerMetadata[nodeId],
        });
        await broker.connect();
        return broker;
    }

    private async findSeedBroker() {
        const randomizedBrokers = this.options.bootstrapServers.toSorted(() => Math.random() - 0.5);
        for (const options of randomizedBrokers) {
            try {
                const broker = new Broker({
                    clientId: this.options.clientId,
                    sasl: this.options.sasl,
                    ssl: this.options.ssl,
                    requestTimeout: this.options.requestTimeout,
                    options,
                });
                await broker.connect();
                return broker;
            } catch (error) {
                log.warn(`Failed to connect to seed broker ${options.host}:${options.port}`, error);
            }
        }
        throw new KafkaTSError('No seed brokers found');
    }
}
