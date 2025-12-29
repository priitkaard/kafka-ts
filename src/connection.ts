import assert from 'assert';
import net, { isIP, Socket, TcpSocketConnectOpts } from 'net';
import tls, { TLSSocketOptions } from 'tls';
import { getApiName } from './api';
import { Api } from './utils/api';
import { cached } from './utils/cached';
import { Decoder } from './utils/decoder';
import { Encoder } from './utils/encoder';
import { ConnectionError, KafkaTSApiError } from './utils/error';
import { log } from './utils/logger';
import { createTracer } from './utils/tracer';

const trace = createTracer('Connection');

type ConnectionOptions = {
    clientId: string | null;
    connection: TcpSocketConnectOpts;
    ssl: TLSSocketOptions | null;
    requestTimeout: number;
};

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

    constructor(private options: ConnectionOptions) {}

    public isConnected() {
        return !this.socket.pending && !this.socket.destroyed;
    }

    @trace()
    public async connect() {
        this.queue = {};
        this.chunks = [];

        const { stack } = new Error();

        await new Promise<void>((resolve, reject) => {
            const { ssl, connection } = this.options;

            this.socket = ssl
                ? tls.connect(
                      {
                          ...connection,
                          ...ssl,
                          ...(connection.host && !isIP(connection.host) && { servername: connection.host }),
                      },
                      resolve,
                  )
                : net.connect(connection, resolve);
            this.socket.setKeepAlive(true, 30_000);
            this.socket.once('error', (error) => {
                reject(new ConnectionError(error.message, stack));
            });
        });
        this.socket.removeAllListeners('error');

        this.socket.on('error', (error) => log.debug('Socket error', { error }));
        this.socket.on('data', (data) => this.handleData(data));
        this.socket.once('close', async () => {
            Object.values(this.queue).forEach(({ reject }) => {
                reject(new ConnectionError('Socket closed unexpectedly', stack));
            });
            this.queue = {};
        });
    }

    @trace()
    public disconnect() {
        this.socket.removeAllListeners();
        return new Promise<void>((resolve) => {
            if (!this.isConnected()) {
                return resolve();
            }
            this.socket.end(resolve);
        });
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

        const encoder = new Encoder()
            .writeInt16(api.apiKey)
            .writeInt16(api.apiVersion)
            .writeInt32(correlationId)
            .writeString(this.options.clientId ?? '');
        if (api.requestHeaderVersion === 2) encoder.writeTagBuffer();

        const request = api.request(encoder, body);
        const requestEncoder = new Encoder().writeInt32(request.getBufferLength()).writeEncoder(request);

        const { stack } = new Error();

        let timeout: NodeJS.Timeout | undefined;
        const { responseDecoder, responseSize } = await new Promise<RawResonse>(async (resolve, reject) => {
            timeout = setTimeout(() => {
                delete this.queue[correlationId];
                reject(new ConnectionError(`${apiName} timed out`, stack));
            }, this.options.requestTimeout);

            try {
                this.queue[correlationId] = { api, resolve, reject };
                await this.write(requestEncoder.value());
            } catch (error) {
                reject(new ConnectionError((error as Error).message, stack));
            }
        });
        clearTimeout(timeout);

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

    private write(buffer: Buffer) {
        return new Promise<void>((resolve, reject) => {
            this.socket.write(buffer, 'binary', (error) => (error ? reject(error) : resolve()));
        });
    }

    private handleData(buffer: Buffer) {
        this.chunks.push(buffer);

        const decoder = new Decoder(Buffer.concat(this.chunks));
        if (!decoder.canReadBytes(4)) return;

        const responseSize = decoder.readInt32();
        if (!decoder.canReadBytes(responseSize)) return;

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
        this.chunks = [];

        const remaining = decoder.read();
        if (remaining.length) this.handleData(remaining);
    }

    private nextCorrelationId() {
        return this.lastCorrelationId++;
    }
}

export type SendRequest = typeof Connection.prototype.sendRequest;
