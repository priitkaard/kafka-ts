import { TcpSocketConnectOpts } from 'net';
import { TLSSocketOptions } from 'tls';
import { API } from './api';
import { Metadata } from './api/metadata';
import { Broker, SASLProvider } from './broker';
import { SendRequest } from './connection';
import { ConnectionError, KafkaTSError } from './utils/error';
import { log } from './utils/logger';
import { shared } from './utils/shared';
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
    }

    public async disconnect() {
        await Promise.all([
            this.seedBroker?.disconnect(),
            ...Object.values(this.brokerById).map((x) => x.disconnect()),
        ]);
    }

    public ensureConnected = shared(async () => {
        if (!this.seedBroker) {
            return this.connect();
        }

        const brokers = [
            {
                broker: this.seedBroker,
                handleError: async (error: Error) => {
                    log.debug(`Failed to connect to seed broker. Reconnecting...`, { reason: error.message });
                    await this.seedBroker?.disconnect();
                    this.seedBroker = await this.findSeedBroker();
                },
            },
            ...Object.entries(this.brokerById).map(([nodeId, broker]) => ({
                broker,
                handleError: async (error: Error) => {
                    log.debug(`Failed to connect to broker ${nodeId}. Disconnecting...`, { reason: error.message });
                    await broker.disconnect();
                    delete this.brokerById[parseInt(nodeId)];
                },
            })),
        ];

        await Promise.all(
            brokers.map(async ({ broker, handleError }) => {
                try {
                    await broker.connect();
                } catch (error) {
                    await handleError(error as Error);
                }
            }),
        );
    });

    public setSeedBroker = async (nodeId: number) => {
        const broker = await this.acquireBroker(nodeId);
        await this.seedBroker?.disconnect();
        this.seedBroker = broker;
    };

    public sendRequest: SendRequest = async (...args) => {
        return this.seedBroker!.sendRequest(...args);
    };

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
        if (!(nodeId in this.brokerMetadata)) await this.refreshBrokerMetadata();
        if (!(nodeId in this.brokerMetadata)) throw new ConnectionError(`Broker ${nodeId} is not available`);

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
                log.debug(`Failed to connect to seed broker ${options.host}:${options.port}`, {
                    reason: (error as Error).message,
                });
            }
        }
        throw new KafkaTSError('No seed brokers found');
    }

    private async refreshBrokerMetadata() {
        const metadata = await this.sendRequest(API.METADATA, { topics: [] });
        this.brokerMetadata = Object.fromEntries(metadata.brokers.map((options) => [options.nodeId, options]));
    }
}
