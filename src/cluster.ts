import { TcpSocketConnectOpts } from 'net';
import { TLSSocketOptions } from 'tls';
import { API } from './api';
import { Broker, SASLProvider } from './broker';
import { SendRequest } from './connection';
import { KafkaTSError } from './utils/error';
import { log } from './utils/logger';

type ClusterOptions = {
    clientId: string | null;
    bootstrapServers: TcpSocketConnectOpts[];
    sasl: SASLProvider | null;
    ssl: TLSSocketOptions | null;
};

export class Cluster {
    private seedBroker = new Broker({ clientId: null, sasl: null, ssl: null, options: { port: 9092 } });
    private brokers: { nodeId: number; broker: Broker }[] = [];
    private brokerMetadata: Record<number, Awaited<ReturnType<(typeof API.METADATA)['response']>>['brokers'][number]> =
        {};

    constructor(private options: ClusterOptions) {}

    public async connect() {
        this.seedBroker = await this.findSeedBroker();

        const metadata = await this.sendRequest(API.METADATA, {
            allowTopicAutoCreation: false,
            includeTopicAuthorizedOperations: false,
            topics: [],
        });
        this.brokerMetadata = Object.fromEntries(metadata.brokers.map((options) => [options.nodeId, options]));
    }

    public async disconnect() {
        await Promise.all(this.brokers.map((x) => x.broker.disconnect()));
    }

    public setSeedBroker = async (nodeId: number) => {
        await this.releaseBroker(this.seedBroker);
        this.seedBroker = await this.acquireBroker(nodeId);
    };

    public sendRequest: SendRequest = (...args) => this.seedBroker.sendRequest(...args);

    public sendRequestToNode =
        (nodeId: number): SendRequest =>
        async (...args) => {
            let broker = this.brokers.find((x) => x.nodeId === nodeId)?.broker;
            if (!broker) {
                broker = await this.acquireBroker(nodeId);
            }
            return broker.sendRequest(...args);
        };

    public async acquireBroker(nodeId: number) {
        const broker = new Broker({
            clientId: this.options.clientId,
            sasl: this.options.sasl,
            ssl: this.options.ssl,
            options: this.brokerMetadata[nodeId],
        });
        this.brokers.push({ nodeId, broker });
        await broker.connect();
        return broker;
    }

    public async releaseBroker(broker: Broker) {
        await broker.disconnect();
        this.brokers = this.brokers.filter((x) => x.broker !== broker);
    };

    private async findSeedBroker() {
        const randomizedBrokers = this.options.bootstrapServers.toSorted(() => Math.random() - 0.5);
        for (const options of randomizedBrokers) {
            try {
                const broker = await new Broker({
                    clientId: this.options.clientId,
                    sasl: this.options.sasl,
                    ssl: this.options.ssl,
                    options,
                });
                await broker.connect();
                this.brokers.push({ nodeId: -1, broker });
                return broker;
            } catch (error) {
                log.warn(`Failed to connect to seed broker ${options.host}:${options.port}`, error);
            }
        }
        throw new KafkaTSError('No seed brokers found');
    }
}
