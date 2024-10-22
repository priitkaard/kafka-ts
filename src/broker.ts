import { TcpSocketConnectOpts } from 'net';
import { TLSSocketOptions } from 'tls';
import { API } from './api';
import { Connection, SendRequest } from './connection';
import { base64Decode, base64Encode, generateNonce, hash, hmac, saltPassword, xor } from './utils/crypto';
import { KafkaTSError } from './utils/error';
import { memo } from './utils/memo';

export type SASLOptions = {
    mechanism: 'PLAIN' | 'SCRAM-SHA-256';
    username: string;
    password: string;
};

type BrokerOptions = {
    clientId: string | null;
    options: TcpSocketConnectOpts;
    sasl: SASLOptions | null;
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

    public ensureConnected = memo(() => this.connect());

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
                throw new KafkaTSError(`API ${apiKey} version ${apiVersion} is not supported by the broker`);
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
        if (!this.options.sasl) {
            return;
        }

        const { mechanism } = this.options.sasl;
        const authenticate = { PLAIN: plainProvider, 'SCRAM-SHA-256': scramSha256Provider }[mechanism as string];
        if (!authenticate) {
            throw new KafkaTSError(`SASL mechanism ${mechanism} is not supported`);
        }

        await authenticate({
            ...this.options.sasl,
            sendRequest: this.sendRequest.bind(this),
        });
    }
}

const plainProvider = async ({
    username,
    password,
    sendRequest,
}: {
    username: string;
    password: string;
    sendRequest: SendRequest;
}) => {
    const authBytes = [null, username, password].join('\u0000');
    await sendRequest(API.SASL_AUTHENTICATE, { authBytes: Buffer.from(authBytes) });
};

const scramSha256Provider = async ({
    username,
    password,
    sendRequest,
}: {
    username: string;
    password: string;
    sendRequest: SendRequest;
}) => {
    const nonce = generateNonce();
    const firstMessage = `n=${username},r=${nonce}`;

    const { authBytes } = await sendRequest(API.SASL_AUTHENTICATE, { authBytes: Buffer.from(`n,,${firstMessage}`) });
    if (!authBytes) {
        throw new KafkaTSError('No auth response');
    }

    const response = Object.fromEntries(
        authBytes
            .toString()
            .split(',')
            .map((pair) => pair.split('=')),
    ) as { r: string; s: string; i: string };

    const rnonce = response.r;
    if (!rnonce.startsWith(nonce)) {
        throw new KafkaTSError('Invalid nonce');
    }
    const iterations = parseInt(response.i);
    const salt = base64Decode(response.s);

    const saltedPassword = await saltPassword(password, salt, iterations, 32, 'sha256');
    const clientKey = hmac(saltedPassword, 'Client Key');
    const clientKeyHash = hash(clientKey);

    let finalMessage = `c=${base64Encode('n,,')},r=${rnonce}`;

    const fullMessage = `${firstMessage},${authBytes.toString()},${finalMessage}`;
    const clientSignature = hmac(clientKeyHash, fullMessage);
    const clientProof = base64Encode(xor(clientKey, clientSignature));

    finalMessage += `,p=${clientProof}`;

    await sendRequest(API.SASL_AUTHENTICATE, { authBytes: Buffer.from(finalMessage) });
};
