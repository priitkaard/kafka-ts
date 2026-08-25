import assert from 'assert';
import net, { isIP, Socket, TcpSocketConnectOpts } from 'net';
import tls, { TLSSocketOptions } from 'tls';
import { getApiName } from './api';
import { Api } from './utils/api';
import { cached } from './utils/cached';
import { Decoder } from './utils/decoder';
import { Encoder } from './utils/encoder';
import { ConnectionError, getErrorMessage, KafkaTSApiError } from './utils/error';
import { log } from './utils/logger';
import { createTracer } from './utils/tracer';

const trace = createTracer('Connection');

type ConnectionOptions = {
    clientId: string | null;
    connection: TcpSocketConnectOpts;
    ssl: TLSSocketOptions | null;
    requestTimeout: number;
    connectTimeout: number;
};

const SOCKET_CLOSE_TIMEOUT_MS = 5_000;

type RawResonse = { responseDecoder: Decoder; responseSize: number };

type Versions = { [apiKey: number]: { minVersion: number; maxVersion: number } };

export class Connection {
    private socket = new Socket();
    private queue: {
        [correlationId: number]: {
            api: Api<any, any>;
            resolve: (response: RawResonse) => void;
            reject: (error: Error) => void;
        };
    } = {};
    private lastCorrelationId = 0;
    private chunks: Buffer[] = [];
    private versions: Versions | undefined;
    private connected = false;
    private connecting: Promise<void> | undefined;
    private closing: Promise<void> | undefined;
    private generation = 0;

    constructor(private options: ConnectionOptions) {}

    public isConnected() {
        return this.connected;
    }

    @trace()
    public async connect() {
        this.connecting ??= this.establishConnection().finally(() => {
            this.connecting = undefined;
        });
        return this.connecting;
    }

    private async establishConnection() {
        const generation = this.generation;

        await this.teardown();

        this.chunks = [];

        const { stack } = new Error();
        const { ssl, connection, connectTimeout } = this.options;
        const address = `${connection.host}:${connection.port}`;
        const readyEvent = ssl ? 'secureConnect' : 'connect';

        const socket: Socket = ssl
            ? tls.connect({
                  ...connection,
                  ...ssl,
                  ...(connection.host && !isIP(connection.host) && { servername: connection.host }),
              })
            : net.connect(connection);

        this.socket = socket;
        this.closing = undefined;
        socket.setKeepAlive(true, 30_000);

        await new Promise<void>((resolve, reject) => {
            let settled = false;

            const finish = (callback: () => void) => {
                if (settled) return;
                settled = true;
                clearTimeout(timeout);
                socket.removeListener(readyEvent, onReady);
                socket.removeListener('error', onError);
                callback();
            };
            const onReady = () => finish(resolve);
            const onError = (error: Error) =>
                finish(() => {
                    socket.destroy();
                    reject(new ConnectionError(getErrorMessage(error), stack));
                });

            const timeout = setTimeout(
                () =>
                    finish(() => {
                        socket.destroy();
                        reject(new ConnectionError(`Connection to ${address} timed out`, stack));
                    }),
                connectTimeout,
            );

            socket.once(readyEvent, onReady);
            socket.once('error', onError);
        });

        if (generation !== this.generation) {
            socket.destroy();
            throw new ConnectionError(`Connection to ${address} was closed while connecting`, stack);
        }

        this.connected = true;

        socket.on('error', (error) => {
            log.debug('Socket error', { error });
            this.handleDisconnect(socket, new ConnectionError(getErrorMessage(error), stack));
        });
        socket.on('data', (data: Buffer) => this.handleData(data));
        socket.once('close', () => {
            this.handleDisconnect(socket, new ConnectionError('Socket closed unexpectedly', stack));
        });
    }

    @trace()
    public async disconnect() {
        this.generation++;
        return this.teardown();
    }

    private async teardown() {
        this.connected = false;
        this.rejectPendingRequests(new ConnectionError('Connection closed'));

        this.closing ??= this.closeSocket(this.socket);
        return this.closing;
    }

    private async closeSocket(socket: Socket) {
        socket.removeAllListeners('data');
        socket.removeAllListeners('close');
        socket.removeAllListeners('error');
        socket.on('error', () => {});

        if (socket.destroyed) return;

        await new Promise<void>((resolve) => {
            const timeout = setTimeout(resolve, SOCKET_CLOSE_TIMEOUT_MS);
            socket.end(() => {
                clearTimeout(timeout);
                resolve();
            });
        });
        socket.destroy();
    }

    private handleDisconnect(socket: Socket, error: ConnectionError) {
        if (socket !== this.socket) return;
        this.connected = false;
        this.rejectPendingRequests(error);
    }

    private rejectPendingRequests(error: ConnectionError) {
        const queue = this.queue;
        this.queue = {};
        Object.values(queue).forEach(({ reject }) => reject(error));
    }

    public setVersions(versions: Versions) {
        this.versions = versions;
        this.validateVersionCached.clear();
    }

    private validateVersion<Request, Response>(api: Api<Request, Response>): Api<Request, Response> {
        if (!this.versions) return api;

        const versionInfo = this.versions[api.apiKey];
        if (!versionInfo) throw new Error(`Broker does not support API ${getApiName(api)}`);

        if (api.apiVersion < versionInfo.minVersion || api.apiVersion > versionInfo.maxVersion) {
            if (api.fallback) {
                return this.validateVersion(api.fallback);
            }
            throw new Error(
                `Broker does not support API ${getApiName(api)} version ${api.apiVersion} (minVersion=${versionInfo.minVersion}, maxVersion=${versionInfo.maxVersion})`,
            );
        }

        log.debug(`Using API ${getApiName(api)} version ${api.apiVersion}`);
        return api;
    }

    private validateVersionCached = cached(this.validateVersion.bind(this), (api) => api.apiKey.toString());

    @trace((api, body) => ({ message: getApiName(api), body }))
    public async sendRequest<Request, Response>(apiLatest: Api<Request, Response>, body: Request): Promise<Response> {
        const api = this.validateVersionCached(apiLatest);
        const correlationId = this.nextCorrelationId();
        const apiName = getApiName(api);

        if (!this.connected) {
            const { host, port } = this.options.connection;
            throw new ConnectionError(`Not connected to ${host}:${port} (${apiName})`);
        }

        const encoder = new Encoder()
            .writeInt16(api.apiKey)
            .writeInt16(api.apiVersion)
            .writeInt32(correlationId)
            .writeString(this.options.clientId ?? '');
        if (api.requestHeaderVersion === 2) encoder.writeTagBuffer();

        const request = api.request(encoder, body);
        const requestEncoder = new Encoder().writeInt32(request.getBufferLength()).writeEncoder(request);

        const { stack } = new Error();

        const socket = this.socket;

        let timeout: NodeJS.Timeout | undefined;
        let rawResponse: RawResonse;
        try {
            rawResponse = await new Promise<RawResonse>(async (resolve, reject) => {
                timeout = setTimeout(() => {
                    delete this.queue[correlationId];
                    reject(new ConnectionError(`${apiName} timed out`, stack));
                }, this.options.requestTimeout);

                try {
                    this.queue[correlationId] = { api, resolve, reject };
                    await this.write(socket, requestEncoder.value());
                } catch (error) {
                    delete this.queue[correlationId];
                    reject(new ConnectionError(getErrorMessage(error), stack));
                }
            });
        } finally {
            clearTimeout(timeout);
        }
        const { responseDecoder, responseSize } = rawResponse;

        try {
            const response = await api.response(responseDecoder);

            assert(
                responseDecoder.getOffset() === responseSize,
                `Buffer not correctly consumed: ${responseDecoder.getOffset()} !== ${responseSize}`,
            );

            return response;
        } catch (error) {
            if (error instanceof KafkaTSApiError) {
                error.apiName = apiName;
                error.request = body;
            }
            throw error;
        }
    }

    private write(socket: Socket, buffer: Buffer) {
        return new Promise<void>((resolve, reject) => {
            socket.write(buffer, 'binary', (error) => (error ? reject(error) : resolve()));
        });
    }

    private handleData(buffer: Buffer) {
        try {
            this.consume(buffer);
        } catch (error) {
            const socket = this.socket;
            log.debug('Failed to read response', { error });

            this.handleDisconnect(socket, new ConnectionError(`Failed to read response: ${getErrorMessage(error)}`));
            socket.destroy();
        }
    }

    private consume(buffer: Buffer) {
        this.chunks.push(buffer);

        let remaining: Buffer = Buffer.concat(this.chunks);
        this.chunks = [];

        while (true) {
            const decoder = new Decoder(remaining);
            if (!decoder.canReadBytes(4)) break;

            const responseSize = decoder.readInt32();
            if (responseSize < 0) {
                throw new ConnectionError(`Invalid response size: ${responseSize}`);
            }
            if (!decoder.canReadBytes(responseSize)) break;

            const responseDecoder = new Decoder(decoder.read(responseSize));
            const correlationId = responseDecoder.readInt32();

            const context = this.queue[correlationId];
            if (context?.api.responseHeaderVersion === 1) responseDecoder.readTagBuffer();

            if (context) {
                delete this.queue[correlationId];
                context.resolve({ responseDecoder, responseSize });
            } else {
                log.debug('Could not find pending request for correlationId', { correlationId });
            }

            remaining = decoder.read();
        }

        if (remaining.length) this.chunks.push(remaining);
    }

    private nextCorrelationId() {
        const correlationId = this.lastCorrelationId;
        this.lastCorrelationId = (this.lastCorrelationId + 1) % 0x7fffffff;
        return correlationId;
    }
}

export type SendRequest = typeof Connection.prototype.sendRequest;
