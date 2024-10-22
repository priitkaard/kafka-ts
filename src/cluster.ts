import { TcpSocketConnectOpts } from 'net';
import { TLSSocketOptions } from 'tls';
import { API } from './api';
import { Broker, SASLProvider } from './broker';
import { SendRequest } from './connection';
import { ConnectionError, KafkaTSError } from './utils/error';

type ClusterOptions = {
    clientId: string | null;
    bootstrapServers: TcpSocketConnectOpts[];
    sasl: SASLProvider | null;
    ssl: TLSSocketOptions | null;
};

export class Cluster {
    private seedBroker: Broker;
    private brokerById: Record<number, Broker> = {};

    constructor(private options: ClusterOptions) {
        this.seedBroker = new Broker({
            clientId: this.options.clientId,
            sasl: this.options.sasl,
            ssl: this.options.ssl,
            options: this.options.bootstrapServers[0],
        });
    }

    public async connect() {
        await this.connectSeedBroker();
        const metadata = await this.sendRequest(API.METADATA, {
            allowTopicAutoCreation: false,
            includeTopicAuthorizedOperations: false,
            topics: [],
        });

        this.brokerById = Object.fromEntries(
            metadata.brokers.map(({ nodeId, ...options }) => [
                nodeId,
                new Broker({
                    clientId: this.options.clientId,
                    sasl: this.options.sasl,
                    ssl: this.options.ssl,
                    options,
                }),
            ]),
        );
        return this;
    }

    public async disconnect() {
        await Promise.all([
            this.seedBroker.disconnect(),
            ...Object.values(this.brokerById).map((broker) => broker.disconnect()),
        ]);
    }

    public sendRequest: SendRequest = (...args) => this.seedBroker.sendRequest(...args);

    public sendRequestToNode =
        (nodeId: number): SendRequest =>
        async (...args) => {
            const broker = this.brokerById[nodeId];
            if (!broker) {
                throw new ConnectionError(`Broker ${nodeId} is not available`);
            }
            await broker.ensureConnected();
            return broker.sendRequest(...args);
        };

    private async connectSeedBroker() {
        const randomizedBrokers = this.options.bootstrapServers.toSorted(() => Math.random() - 0.5);
        for (const options of randomizedBrokers) {
            try {
                this.seedBroker = await new Broker({
                    clientId: this.options.clientId,
                    sasl: this.options.sasl,
                    ssl: this.options.ssl,
                    options,
                }).connect();
                return;
            } catch (error) {
                console.warn(`Failed to connect to seed broker ${options.host}:${options.port}`, error);
            }
        }
        throw new KafkaTSError('No seed brokers found');
    }
}
