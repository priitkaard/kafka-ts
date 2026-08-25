import net, { AddressInfo, Server, Socket } from 'net';
import { afterEach, describe, expect, it } from 'vitest';
import { API } from './api';
import { Connection } from './connection';
import { ConnectionError } from './utils/error';

const createConnection = (port: number, overrides: Partial<{ connectTimeout: number }> = {}) =>
    new Connection({
        clientId: 'kafka-ts-test',
        connection: { host: '127.0.0.1', port },
        ssl: null,
        requestTimeout: 1_000,
        connectTimeout: 1_000,
        ...overrides,
    });

describe('Connection', () => {
    let servers: Server[] = [];
    let sockets: Socket[] = [];

    const startServer = async (onConnection: (socket: Socket) => void) => {
        const server = net.createServer({ allowHalfOpen: true }, (socket) => {
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

    it('is not connected after disconnect, even when the peer never closes its side', async () => {
        const port = await startServer((socket) => socket.on('end', () => {}));

        const connection = createConnection(port);
        await connection.connect();
        expect(connection.isConnected()).toBe(true);

        await connection.disconnect();
        await new Promise((resolve) => setTimeout(resolve, 100));

        expect(connection.isConnected()).toBe(false);
    });

    it('reconnects a connection that was previously disconnected', async () => {
        const port = await startServer((socket) => socket.on('end', () => {}));

        const connection = createConnection(port);
        await connection.connect();
        await connection.disconnect();

        await connection.connect();
        expect(connection.isConnected()).toBe(true);

        await connection.disconnect();
    });

    it('marks the connection as disconnected when the peer closes the socket', async () => {
        const port = await startServer((socket) => socket.destroy());

        const connection = createConnection(port);
        await connection.connect();
        await new Promise((resolve) => setTimeout(resolve, 100));

        expect(connection.isConnected()).toBe(false);
    });

    it('rejects in-flight requests when the socket goes away', async () => {
        const port = await startServer((socket) => {
            socket.on('data', () => socket.destroy());
        });

        const connection = createConnection(port);
        await connection.connect();

        await expect(connection.sendRequest(API.API_VERSIONS, {})).rejects.toThrow(ConnectionError);
    });

    it('fails fast instead of writing to a disconnected socket', async () => {
        const port = await startServer((socket) => socket.on('end', () => {}));

        const connection = createConnection(port);
        await connection.connect();
        await connection.disconnect();

        await expect(connection.sendRequest(API.API_VERSIONS, {})).rejects.toThrow(ConnectionError);
    });

    it('does not emit an unhandled error event when the peer resets during teardown', async () => {
        const port = await startServer((socket) => {
            socket.on('end', () => socket.resetAndDestroy());
            socket.on('error', () => {});
        });

        const connection = createConnection(port);
        await connection.connect();

        const uncaught: Error[] = [];
        const onUncaught = (error: Error) => uncaught.push(error);
        process.on('uncaughtException', onUncaught);
        try {
            await connection.disconnect();
            await new Promise((resolve) => setTimeout(resolve, 200));
        } finally {
            process.off('uncaughtException', onUncaught);
        }

        expect(uncaught).toEqual([]);
    });

    it('rejects in-flight requests when the connection is re-established', async () => {
        const port = await startServer((socket) => socket.on('data', () => {}));

        const connection = createConnection(port);
        await connection.connect();

        const inFlight = connection.sendRequest(API.API_VERSIONS, {});
        await connection.connect();

        await expect(inFlight).rejects.toThrow(ConnectionError);

        await connection.disconnect();
    });

    it('does not leave request timers behind when a request fails', async () => {
        const port = await startServer((socket) => {
            socket.on('data', () => socket.destroy());
        });

        const connection = createConnection(port);
        await connection.connect();

        const countTimers = () => process.getActiveResourcesInfo().filter((resource) => resource === 'Timeout').length;

        const timersBefore = countTimers();
        await expect(connection.sendRequest(API.API_VERSIONS, {})).rejects.toThrow(ConnectionError);

        expect(countTimers()).toBe(timersBefore);
    });

    it('detaches listeners from the previous socket when reconnecting', async () => {
        const port = await startServer(() => {});

        const connection = createConnection(port);
        await connection.connect();

        const firstSocket = (connection as any).socket as Socket;
        (connection as any).connected = false;

        await connection.connect();

        expect((connection as any).socket).not.toBe(firstSocket);
        expect(firstSocket.listenerCount('data')).toBe(0);
        expect(firstSocket.destroyed).toBe(true);

        await connection.disconnect();
    });

    it('opens a single socket for concurrent connects', async () => {
        const port = await startServer(() => {});

        const connection = createConnection(port);
        await Promise.all([connection.connect(), connection.connect(), connection.connect()]);
        await new Promise((resolve) => setTimeout(resolve, 100));

        expect(sockets.length).toBe(1);
        expect(connection.isConnected()).toBe(true);

        await connection.disconnect();
    });

    it('does not crash the process on a malformed response frame', async () => {
        const port = await startServer((socket) => {
            socket.on('data', () => {
                const frame = Buffer.alloc(4);
                frame.writeInt32BE(-1);
                socket.write(frame);
            });
        });

        const connection = createConnection(port);
        await connection.connect();

        const uncaught: Error[] = [];
        const onUncaught = (error: Error) => uncaught.push(error);
        process.on('uncaughtException', onUncaught);
        try {
            await expect(connection.sendRequest(API.API_VERSIONS, {})).rejects.toThrow(ConnectionError);
            await new Promise((resolve) => setTimeout(resolve, 100));
        } finally {
            process.off('uncaughtException', onUncaught);
        }

        expect(uncaught).toEqual([]);
        expect(connection.isConnected()).toBe(false);
    });

    it('reassembles a response split across packets', async () => {
        const body = Buffer.alloc(20);
        body.writeInt32BE(0, 0);
        body.writeInt16BE(0, 4);
        body.writeInt32BE(1, 6);
        body.writeInt16BE(18, 10);
        body.writeInt16BE(0, 12);
        body.writeInt16BE(3, 14);
        body.writeInt32BE(0, 16);

        const size = Buffer.alloc(4);
        size.writeInt32BE(body.length);
        const frame = Buffer.concat([size, body]);

        const port = await startServer((socket) => {
            socket.on('data', () => {
                socket.write(frame.subarray(0, 6));
                setTimeout(() => socket.write(frame.subarray(6, 11)), 20);
                setTimeout(() => socket.write(frame.subarray(11)), 40);
            });
        });

        const connection = createConnection(port);
        await connection.connect();

        await expect(connection.sendRequest(API.API_VERSIONS, {})).resolves.toEqual({
            errorCode: 0,
            versions: [{ apiKey: 18, minVersion: 0, maxVersion: 3 }],
            throttleTimeMs: 0,
        });

        await connection.disconnect();
    });

    it('does not report a connection that was disconnected while connecting', async () => {
        const port = await startServer(() => {});

        const connection = createConnection(port);
        const connecting = connection.connect();
        await connection.disconnect();
        await connecting.catch(() => {});
        await new Promise((resolve) => setTimeout(resolve, 100));

        expect(connection.isConnected()).toBe(false);
        expect((connection as any).socket.destroyed).toBe(true);
    });

    it('reports the underlying reason when a host resolves to several addresses', async () => {
        const server = net.createServer();
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
        const { port } = server.address() as AddressInfo;
        await new Promise((resolve) => server.close(resolve));

        const connection = new Connection({
            clientId: 'kafka-ts-test',
            connection: { host: 'localhost', port },
            ssl: null,
            requestTimeout: 1_000,
            connectTimeout: 1_000,
        });

        await expect(connection.connect()).rejects.toThrow(/ECONNREFUSED/);
    });

    it('keeps working when the correlation id reaches the int32 limit', async () => {
        const port = await startServer((socket) => {
            socket.on('data', (data) => {
                let offset = 0;
                while (offset + 4 <= data.length) {
                    const size = data.readInt32BE(offset);
                    const correlationId = data.readInt32BE(offset + 8);
                    offset += 4 + size;

                    const body = Buffer.alloc(20);
                    body.writeInt32BE(correlationId, 0);
                    body.writeInt16BE(0, 4);
                    body.writeInt32BE(1, 6);
                    body.writeInt16BE(18, 10);
                    body.writeInt16BE(0, 12);
                    body.writeInt16BE(3, 14);
                    body.writeInt32BE(0, 16);

                    const frame = Buffer.alloc(4);
                    frame.writeInt32BE(body.length);
                    socket.write(Buffer.concat([frame, body]));
                }
            });
        });

        const connection = createConnection(port);
        await connection.connect();
        (connection as any).lastCorrelationId = 2_147_483_645;

        for (let i = 0; i < 4; i++) {
            await expect(connection.sendRequest(API.API_VERSIONS, {})).resolves.toBeTruthy();
        }

        await connection.disconnect();
    });

    it('times out a connect that never completes', async () => {
        const connection = createConnection(9, { connectTimeout: 200 });
        (connection as any).options.connection = { host: '192.0.2.1', port: 9 };

        await expect(connection.connect()).rejects.toThrow(/timed out/);
        expect(connection.isConnected()).toBe(false);
    });
});
