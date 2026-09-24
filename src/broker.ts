import { TcpSocketConnectOpts } from 'net';
import { TLSSocketOptions } from 'tls';
import { API } from './api';
import { Connection, SendRequest } from './connection';
import { ConnectionError } from './utils/error';
import { shared } from './utils/shared';
import { withTimeout } from './utils/timeout';

export type SASLProvider = {
    mechanism: string;
    authenticate: (context: { sendRequest: SendRequest }) => Promise<void>;
};

type BrokerOptions = {
    clientId: string | null;
    options: TcpSocketConnectOpts;
    sasl: SASLProvider | null;
    ssl: TLSSocketOptions | null;
    requestTimeout: number;
    connectTimeout: number;
};

export class Broker {
    private connection: Connection;
    private sendConnectionRequest: SendRequest;
    private ready = false;

    constructor(private options: BrokerOptions) {
        this.connection = new Connection({
            clientId: this.options.clientId,
            connection: this.options.options,
            ssl: this.options.ssl,
            requestTimeout: this.options.requestTimeout,
            connectTimeout: this.options.connectTimeout,
        });
        this.sendConnectionRequest = this.connection.sendRequest.bind(this.connection);
    }

    public sendRequest: SendRequest = async (...args) => {
        if (!this.ready) {
            const { host, port } = this.options.options;
            throw new ConnectionError(`Not connected to ${host}:${port}`);
        }
        return this.sendConnectionRequest(...args);
    };

    public connect = shared(async () => {
        if (this.ready && this.connection.isConnected()) {
            return this;
        }
        this.ready = false;
        await this.connection.connect();

        const { host, port } = this.options.options;
        try {
            await withTimeout(
                this.handshake(),
                this.options.connectTimeout,
                `Handshake with ${host}:${port} timed out`,
            );
        } catch (error) {
            await this.disconnect().catch(() => {});
            throw error;
        }
        this.ready = true;
        return this;
    });

    private async handshake() {
        await this.fetchApiVersions();
        await this.saslHandshake();
        await this.saslAuthenticate();
    }

    public async disconnect() {
        this.ready = false;
        await this.connection.disconnect();
    }

    private async fetchApiVersions() {
        const { versions } = await this.sendConnectionRequest(API.API_VERSIONS, {});
        const versionsByApiKey = Object.fromEntries(
            versions.map(({ apiKey, minVersion, maxVersion }) => [apiKey, { minVersion, maxVersion }]),
        );
        this.connection.setVersions(versionsByApiKey);
    }

    private async saslHandshake() {
        if (!this.options.sasl) {
            return;
        }
        await this.sendConnectionRequest(API.SASL_HANDSHAKE, { mechanism: this.options.sasl.mechanism });
    }

    private async saslAuthenticate() {
        await this.options.sasl?.authenticate({ sendRequest: this.sendConnectionRequest });
    }
}
