import assert from 'assert';
import net, { isIP, Socket, TcpSocketConnectOpts } from 'net';
import tls, { TLSSocketOptions } from 'tls';
import { getApiName } from './api';
import { Api } from './utils/api';
import { Decoder } from './utils/decoder';
import { Encoder } from './utils/encoder';
import { ConnectionError } from './utils/error';
import { log } from './utils/logger';
import { createTracer } from './utils/tracer';

const trace = createTracer('Connection');

export type ConnectionOptions = {
    clientId: string | null;
    connection: TcpSocketConnectOpts;
    ssl: TLSSocketOptions | null;
};

type RawResonse = { responseDecoder: Decoder; responseSize: number };

export class Connection {
    private socket = new Socket();
    private queue: {
        [correlationId: number]: { resolve: (response: RawResonse) => void; reject: (error: Error) => void };
    } = {};
    private lastCorrelationId = 0;
    private chunks: Buffer[] = [];

    constructor(private options: ConnectionOptions) {}

    @trace()
    public async connect() {
        this.queue = {};
        this.chunks = [];

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
            this.socket.once('error', reject);
        });
        this.socket.removeAllListeners('error');

        this.socket.on('data', (data) => this.handleData(data));
        this.socket.once('close', async () => {
            Object.values(this.queue).forEach(({ reject }) => {
                reject(new ConnectionError('Socket closed unexpectedly'));
            });
            this.queue = {};
        });
    }

    @trace()
    public disconnect() {
        this.socket.removeAllListeners();
        return new Promise<void>((resolve) => {
            if (this.socket.pending) {
                return resolve();
            }
            this.socket.end(resolve);
        });
    }

    @trace((api, body) => ({ message: getApiName(api), body }))
    public async sendRequest<Request, Response>(api: Api<Request, Response>, body: Request): Promise<Response> {
        const correlationId = this.nextCorrelationId();
        const apiName = getApiName(api);

        const encoder = new Encoder()
            .writeInt16(api.apiKey)
            .writeInt16(api.apiVersion)
            .writeInt32(correlationId)
            .writeString(this.options.clientId);

        const request = api.request(encoder, body);
        const requestEncoder = new Encoder().writeInt32(request.getByteLength()).writeEncoder(request);

        let timeout: NodeJS.Timeout | undefined;
        const { responseDecoder, responseSize } = await new Promise<RawResonse>(async (resolve, reject) => {
            timeout = setTimeout(() => {
                delete this.queue[correlationId];
                reject(new ConnectionError(`${apiName} timed out`));
            }, 30_000);

            try {
                this.queue[correlationId] = { resolve, reject };
                await this.write(requestEncoder.value());
            } catch (error) {
                reject(error);
            }
        });
        clearTimeout(timeout);
        const response = await api.response(responseDecoder);

        assert(
            responseDecoder.getOffset() - 4 === responseSize,
            `Buffer not correctly consumed: ${responseDecoder.getOffset() - 4} !== ${responseSize}`,
        );

        return response;
    }

    private write(buffer: Buffer) {
        return new Promise<void>((resolve, reject) => {
            const { stack } = new Error('Write error');
            this.socket.write(buffer, (error) => {
                if (error) {
                    const err = new ConnectionError(error.message);
                    err.stack += `\n${stack}`;
                    return reject(err);
                }
                resolve();
            });
        });
    }

    private handleData(buffer: Buffer) {
        this.chunks.push(buffer);

        const decoder = new Decoder(Buffer.concat(this.chunks));
        if (decoder.getBufferLength() < 4) {
            return;
        }

        const size = decoder.readInt32();
        if (size !== decoder.getBufferLength() - 4) {
            return;
        }

        const correlationId = decoder.readInt32();

        const context = this.queue[correlationId];
        if (context) {
            delete this.queue[correlationId];
            context.resolve({ responseDecoder: decoder, responseSize: size });
        } else {
            log.debug('Could not find pending request for correlationId', { correlationId });
        }
        this.chunks = [];
    }

    private nextCorrelationId() {
        return this.lastCorrelationId++;
    }
}

export type SendRequest = typeof Connection.prototype.sendRequest;
