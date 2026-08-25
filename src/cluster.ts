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
    connectTimeout: number;
};

export class Cluster {
    private seedBroker: Broker | undefined;
    private brokerById: Record<number, Broker> = {};
    private brokerMetadata: Record<number, Metadata['brokers'][number]> = {};

    constructor(private options: ClusterOptions) {}

    public async connect() {
        const seedBroker = await this.findSeedBroker();

        const staleBrokers = [this.seedBroker, ...Object.values(this.brokerById)];
        this.seedBroker = seedBroker;
        this.brokerById = {};
        await Promise.all(staleBrokers.map((broker) => this.safeDisconnect(broker)));

        try {
            await this.refreshBrokerMetadata();
        } catch (error) {
            this.seedBroker = undefined;
            await this.safeDisconnect(seedBroker);
            throw error;
        }
    }

    public async disconnect() {
        const brokers = [this.seedBroker, ...Object.values(this.brokerById)];

        this.seedBroker = undefined;
        this.brokerById = {};
        this.brokerMetadata = {};

        await Promise.all(brokers.map((broker) => this.safeDisconnect(broker)));
    }

    public ensureConnected = shared(async () => {
        if (!this.seedBroker) {
            return this.connect();
        }

        try {
            await this.seedBroker.connect();
        } catch (error) {
            log.debug(`Failed to connect to seed broker. Reconnecting...`, { reason: (error as Error).message });

            const staleBroker = this.seedBroker;
            this.seedBroker = undefined;
            this.brokerMetadata = {};
            await this.safeDisconnect(staleBroker);

            return this.connect();
        }

        await Promise.all(
            Object.entries(this.brokerById).map(async ([nodeId, broker]) => {
                try {
                    await broker.connect();
                } catch (error) {
                    log.debug(`Failed to connect to broker ${nodeId}. Disconnecting...`, {
                        reason: (error as Error).message,
                    });
                    await this.evictBroker(parseInt(nodeId), broker);
                }
            }),
        );
    });

    public setSeedBroker = async (nodeId: number) => {
        const broker = await this.acquireBroker(nodeId);
        const staleBroker = this.seedBroker;
        this.seedBroker = broker;
        await this.safeDisconnect(staleBroker);
    };

    public sendRequest: SendRequest = async (...args) => {
        if (!this.seedBroker) {
            throw new ConnectionError('Cluster is not connected');
        }
        return this.seedBroker.sendRequest(...args);
    };

    public sendRequestToNode =
        (nodeId: number): SendRequest =>
        async (...args) => {
            const broker = await this.getBroker(nodeId);
            try {
                return await broker.sendRequest(...args);
            } catch (error) {
                if (error instanceof ConnectionError) {
                    await this.evictBroker(nodeId, broker);
                }
                throw error;
            }
        };

    private acquireBrokerShared = shared((nodeId: number) => this.acquireBroker(nodeId));

    private async getBroker(nodeId: number) {
        const existingBroker = this.brokerById[nodeId];
        if (existingBroker) return existingBroker;

        const broker = await this.acquireBrokerShared(nodeId);

        const currentBroker = this.brokerById[nodeId];
        if (currentBroker && currentBroker !== broker) {
            await this.safeDisconnect(broker);
            return currentBroker;
        }

        this.brokerById[nodeId] = broker;
        return broker;
    }

    private async evictBroker(nodeId: number, broker: Broker) {
        if (this.brokerById[nodeId] !== broker) return;

        delete this.brokerById[nodeId];
        await this.safeDisconnect(broker);
    }

    @trace((nodeId) => ({ nodeId, result: `<Broker ${nodeId}>` }))
    public async acquireBroker(nodeId: number) {
        if (!(nodeId in this.brokerMetadata)) await this.refreshBrokerMetadata();
        if (!(nodeId in this.brokerMetadata)) throw new ConnectionError(`Broker ${nodeId} is not available`);

        const broker = new Broker({
            clientId: this.options.clientId,
            sasl: this.options.sasl,
            ssl: this.options.ssl,
            requestTimeout: this.options.requestTimeout,
            connectTimeout: this.options.connectTimeout,
            options: this.brokerMetadata[nodeId],
        });
        try {
            await broker.connect();
        } catch (error) {
            await this.safeDisconnect(broker);
            throw error;
        }
        return broker;
    }

    private async findSeedBroker() {
        const randomizedBrokers = this.options.bootstrapServers.toSorted(() => Math.random() - 0.5);
        for (const options of randomizedBrokers) {
            const broker = new Broker({
                clientId: this.options.clientId,
                sasl: this.options.sasl,
                ssl: this.options.ssl,
                requestTimeout: this.options.requestTimeout,
                connectTimeout: this.options.connectTimeout,
                options,
            });
            try {
                await broker.connect();
                return broker;
            } catch (error) {
                await this.safeDisconnect(broker);

                log.warn(`Failed to connect to seed broker ${options.host}:${options.port}`, {
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

    private async safeDisconnect(broker: Broker | undefined) {
        await broker?.disconnect().catch((error) => {
            log.debug('Failed to disconnect broker', { reason: (error as Error).message });
        });
    }
}
