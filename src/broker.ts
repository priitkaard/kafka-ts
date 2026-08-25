import { TcpSocketConnectOpts } from 'net';
import { TLSSocketOptions } from 'tls';
import { API } from './api';
import { Connection, SendRequest } from './connection';
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
    public sendRequest: SendRequest;

    constructor(private options: BrokerOptions) {
        this.connection = new Connection({
            clientId: this.options.clientId,
            connection: this.options.options,
            ssl: this.options.ssl,
            requestTimeout: this.options.requestTimeout,
            connectTimeout: this.options.connectTimeout,
        });
        this.sendRequest = this.connection.sendRequest.bind(this.connection);
    }

    public async connect() {
        if (this.connection.isConnected()) {
            return this;
        }
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
        return this;
    }

    private async handshake() {
        await this.fetchApiVersions();
        await this.saslHandshake();
        await this.saslAuthenticate();
    }

    public async disconnect() {
        await this.connection.disconnect();
    }

    private async fetchApiVersions() {
        const { versions } = await this.sendRequest(API.API_VERSIONS, {});
        const versionsByApiKey = Object.fromEntries(
            versions.map(({ apiKey, minVersion, maxVersion }) => [apiKey, { minVersion, maxVersion }]),
        );
        this.connection.setVersions(versionsByApiKey);
    }

    private async saslHandshake() {
        if (!this.options.sasl) {
            return;
        }
        await this.sendRequest(API.SASL_HANDSHAKE, { mechanism: this.options.sasl.mechanism });
    }

    private async saslAuthenticate() {
        await this.options.sasl?.authenticate({ sendRequest: this.sendRequest });
    }
}
