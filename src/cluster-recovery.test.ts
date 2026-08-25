import net, { AddressInfo, Server, Socket } from 'net';
import { afterEach, describe, expect, it } from 'vitest';
import { API } from './api';
import { Cluster } from './cluster';
import { Encoder } from './utils/encoder';

const API_VERSIONS = 18;
const METADATA = 3;

const encodeFrame = (body: Encoder) => new Encoder().writeInt32(body.getBufferLength()).writeEncoder(body).value();

const apiVersionsResponse = (correlationId: number) =>
    encodeFrame(
        new Encoder()
            .writeInt32(correlationId)
            .writeInt16(0)
            .writeArray(
                [
                    { apiKey: API_VERSIONS, minVersion: 0, maxVersion: 2 },
                    { apiKey: METADATA, minVersion: 0, maxVersion: 0 },
                ],
                (encoder, version) =>
                    encoder.writeInt16(version.apiKey).writeInt16(version.minVersion).writeInt16(version.maxVersion),
            )
            .writeInt32(0),
    );

const metadataResponse = (correlationId: number, port: number) =>
    encodeFrame(
        new Encoder()
            .writeInt32(correlationId)
            .writeArray([{ nodeId: 1, host: '127.0.0.1', port }], (encoder, broker) =>
                encoder.writeInt32(broker.nodeId).writeString(broker.host).writeInt32(broker.port),
            )
            .writeArray([], () => {}),
    );

class FakeBroker {
    private server: Server | undefined;
    private sockets: Socket[] = [];
    public port = 0;

    async start(port = 0) {
        this.server = net.createServer({ allowHalfOpen: true }, (socket) => {
            this.sockets.push(socket);
            socket.on('error', () => {});
            socket.on('data', (data) => this.handleRequest(socket, data));
        });
        await new Promise<void>((resolve) => this.server!.listen(port, '127.0.0.1', resolve));
        this.port = (this.server!.address() as AddressInfo).port;
    }

    async stop() {
        this.sockets.forEach((socket) => socket.destroy());
        this.sockets = [];
        if (this.server) await new Promise((resolve) => this.server!.close(resolve));
        this.server = undefined;
    }

    private handleRequest(socket: Socket, data: Buffer) {
        let offset = 0;
        while (offset + 4 <= data.length) {
            const size = data.readInt32BE(offset);
            const request = data.subarray(offset + 4, offset + 4 + size);
            offset += 4 + size;

            const apiKey = request.readInt16BE(0);
            const correlationId = request.readInt32BE(4);

            if (apiKey === API_VERSIONS) socket.write(apiVersionsResponse(correlationId));
            if (apiKey === METADATA) socket.write(metadataResponse(correlationId, this.port));
        }
    }
}

describe('Cluster recovery', () => {
    let broker: FakeBroker | undefined;

    const createCluster = (port: number) =>
        new Cluster({
            clientId: 'kafka-ts-test',
            bootstrapServers: [{ host: '127.0.0.1', port }],
            sasl: null,
            ssl: null,
            requestTimeout: 2_000,
            connectTimeout: 1_000,
        });

    const stopBroker = async () => {
        await broker!.stop();
        await new Promise((resolve) => setTimeout(resolve, 100));
    };

    afterEach(async () => {
        await broker?.stop();
        broker = undefined;
    });

    it('reconnects after the brokers go away and come back', async () => {
        broker = new FakeBroker();
        await broker.start();
        const { port } = broker;

        const cluster = createCluster(port);
        await cluster.connect();
        await expect(cluster.ensureConnected()).resolves.toBeUndefined();

        await stopBroker();
        await expect(cluster.ensureConnected()).rejects.toThrow(/No seed brokers found/);

        broker = new FakeBroker();
        await broker.start(port);

        await expect(cluster.ensureConnected()).resolves.toBeUndefined();
        await cluster.disconnect();
    });

    it('reconnects after a disconnect the peer never acknowledges', async () => {
        broker = new FakeBroker();
        await broker.start();

        const cluster = createCluster(broker.port);
        await cluster.connect();

        await cluster.disconnect();

        await cluster.ensureConnected();
        await expect(cluster.sendRequest(API.METADATA, { topics: [] })).resolves.toBeTruthy();

        await cluster.disconnect();
    });

    it('recovers repeatedly across several outages', async () => {
        broker = new FakeBroker();
        await broker.start();
        const { port } = broker;

        const cluster = createCluster(port);
        await cluster.connect();

        for (let i = 0; i < 3; i++) {
            await stopBroker();
            await expect(cluster.ensureConnected()).rejects.toThrow();

            broker = new FakeBroker();
            await broker.start(port);
            await expect(cluster.ensureConnected()).resolves.toBeUndefined();
        }

        await cluster.disconnect();
    });
});
