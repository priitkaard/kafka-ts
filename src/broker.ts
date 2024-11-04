import { TcpSocketConnectOpts } from 'net';
import { TLSSocketOptions } from 'tls';
import { API } from './api';
import { Connection, SendRequest } from './connection';
import { KafkaTSError } from './utils/error';

export type SASLProvider = {
    mechanism: string;
    authenticate: (context: { sendRequest: SendRequest }) => Promise<void>;
};

type BrokerOptions = {
    clientId: string | null;
    options: TcpSocketConnectOpts;
    sasl: SASLProvider | null;
    ssl: TLSSocketOptions | null;
};

export class Broker {
    private connection: Connection;
    public sendRequest: SendRequest;

    constructor(private options: BrokerOptions) {
        this.connection = new Connection({
            clientId: this.options.clientId,
            connection: this.options.options,
            ssl: this.options.ssl,
        });
        this.sendRequest = this.connection.sendRequest.bind(this.connection);
    }

    public async connect() {
        await this.connection.connect();
        await this.validateApiVersions();
        await this.saslHandshake();
        await this.saslAuthenticate();
        return this;
    }

    public async ensureConnected() {
        if (!this.connection.isConnected()) {
            await this.connect();
        }
    }

    public async disconnect() {
        await this.connection.disconnect();
    }

    private async validateApiVersions() {
        const { versions } = await this.sendRequest(API.API_VERSIONS, {});

        const apiByKey = Object.fromEntries(Object.values(API).map((api) => [api.apiKey, api]));
        versions.forEach(({ apiKey, minVersion, maxVersion }) => {
            if (!apiByKey[apiKey]) {
                return;
            }
            const { apiVersion } = apiByKey[apiKey];
            if (apiVersion < minVersion || apiVersion > maxVersion) {
                throw new KafkaTSError(`API ${apiKey} version ${apiVersion} is not supported by the broker (minVersion=${minVersion}, maxVersion=${maxVersion})`);
            }
        });
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
