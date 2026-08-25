import net, { AddressInfo, Server, Socket } from 'net';
import { afterEach, describe, expect, it } from 'vitest';
import { Broker } from './broker';

describe('Broker', () => {
    let servers: Server[] = [];
    let sockets: Socket[] = [];

    const startServer = async (onConnection: (socket: Socket) => void) => {
        const server = net.createServer((socket) => {
            sockets.push(socket);
            onConnection(socket);
        });
        servers.push(server);
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        return (server.address() as AddressInfo).port;
    };

    afterEach(async () => {
        sockets.forEach((socket) => socket.destroy());
        sockets = [];
        await Promise.all(servers.map((server) => new Promise((resolve) => server.close(resolve))));
        servers = [];
    });

    it('times out a handshake that never completes instead of waiting for the request timeout', async () => {
        const port = await startServer(() => {});

        const broker = new Broker({
            clientId: 'kafka-ts-test',
            options: { host: '127.0.0.1', port },
            sasl: null,
            ssl: null,
            requestTimeout: 60_000,
            connectTimeout: 300,
        });

        const startedAt = Date.now();
        await expect(broker.connect()).rejects.toThrow(/timed out/);

        expect(Date.now() - startedAt).toBeLessThan(3_000);
    });

    it('closes the socket when the handshake fails', async () => {
        const closed: boolean[] = [];
        const port = await startServer((socket) => {
            socket.resume();
            socket.on('close', () => closed.push(true));
        });

        const broker = new Broker({
            clientId: 'kafka-ts-test',
            options: { host: '127.0.0.1', port },
            sasl: null,
            ssl: null,
            requestTimeout: 60_000,
            connectTimeout: 300,
        });

        await expect(broker.connect()).rejects.toThrow(/timed out/);
        await new Promise((resolve) => setTimeout(resolve, 200));

        expect(closed).toEqual([true]);
    });
});
