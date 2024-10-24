import assert from 'assert';
import net, { isIP, Socket, TcpSocketConnectOpts } from 'net';
import tls, { TLSSocketOptions } from 'tls';
import { getApiName } from './api';
import { Api } from './utils/api';
import { Decoder } from './utils/decoder';
import { Encoder } from './utils/encoder';
import { ConnectionError } from './utils/error';
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
    private buffer: Buffer | null = null;

    constructor(private options: ConnectionOptions) {}

    @trace()
    public async connect() {
        this.queue = {};
        this.buffer = null;

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

        const encoder = new Encoder()
            .writeInt16(api.apiKey)
            .writeInt16(api.apiVersion)
            .writeInt32(correlationId)
            .writeString(this.options.clientId);

        const request = api.request(encoder, body).value();
        const requestEncoder = new Encoder().writeInt32(request.length).write(request);

        const { responseDecoder, responseSize } = await new Promise<RawResonse>(async (resolve, reject) => {
            try {
                await this.write(requestEncoder.value());
                this.queue[correlationId] = { resolve, reject };
            } catch (error) {
                reject(error);
            }
        });
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
        this.buffer = this.buffer ? Buffer.concat([this.buffer, buffer]) : buffer;
        if (this.buffer.length < 4) {
            return;
        }

        const decoder = new Decoder(this.buffer);
        const size = decoder.readInt32();
        if (size !== decoder.getBufferLength() - 4) {
            return;
        }

        const correlationId = decoder.readInt32();

        const { resolve } = this.queue[correlationId];
        delete this.queue[correlationId];

        resolve({ responseDecoder: decoder, responseSize: size });
        this.buffer = null;
    }

    private nextCorrelationId() {
        return (this.lastCorrelationId = (this.lastCorrelationId + 1) % 2 ** 31);
    }
}

export type SendRequest = typeof Connection.prototype.sendRequest;
