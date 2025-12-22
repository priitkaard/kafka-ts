import { TcpSocketConnectOpts } from 'net';
import { TLSSocketOptions } from 'tls';
import { API, getApiName } from './api';
import { Connection, SendRequest } from './connection';
import { Api } from './utils/api';
import { log } from './utils/logger';

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
        });
        this.sendRequest = this.connection.sendRequest.bind(this.connection);
    }

    public async connect() {
        if (!this.connection.isConnected()) {
            await this.connection.connect();
            await this.validateApiVersions();
            await this.saslHandshake();
            await this.saslAuthenticate();
        }
        return this;
    }

    public async disconnect() {
        await this.connection.disconnect();
    }

    private async validateApiVersions() {
        const { versions } = await this.sendRequest(API.API_VERSIONS, {});

        const apiByKey: Record<number, Api<any, any>> = Object.fromEntries(Object.values(API).map((api) => [api.apiKey, api]));
        versions.forEach(({ apiKey, minVersion, maxVersion }) => {
            const api = apiByKey[apiKey];
            if (!api) {
                return;
            }
            if (api.apiVersion < minVersion || api.apiVersion > maxVersion) {
                log.warn(
                    `Broker does not support API ${getApiName(api)} version ${api.apiVersion} (minVersion=${minVersion}, maxVersion=${maxVersion})`,
                );
            }
        });

        this.connection.setVersions(versions);
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
