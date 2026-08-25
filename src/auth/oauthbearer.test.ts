import http, { Server } from 'http';
import { AddressInfo } from 'net';
import { afterEach, describe, expect, it } from 'vitest';
import { oAuthAuthenticator } from './oauthbearer';

describe('oAuthAuthenticator', () => {
    let server: Server | undefined;

    const startTokenEndpoint = async (handler: () => { status: number; body?: unknown }) => {
        server = http.createServer((_, res) => {
            const { status, body } = handler();
            res.writeHead(status, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(body ?? { error: 'unavailable' }));
        });
        await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
        return `http://127.0.0.1:${(server!.address() as AddressInfo).port}/token`;
    };

    afterEach(async () => {
        if (server) await new Promise((resolve) => server!.close(resolve));
        server = undefined;
    });

    it('recovers once the token endpoint comes back, without an unhandled rejection', async () => {
        let attempts = 0;
        const endpoint = await startTokenEndpoint(() => {
            attempts++;
            return attempts <= 5
                ? { status: 503 }
                : { status: 200, body: { access_token: 'token', refresh_token: 'refresh', expires_in: 3600 } };
        });

        const rejections: unknown[] = [];
        const onRejection = (error: unknown) => rejections.push(error);
        process.on('unhandledRejection', onRejection);

        try {
            const getToken = oAuthAuthenticator({ endpoint, clientId: 'id', clientSecret: 'secret' });

            await expect(getToken()).rejects.toThrow(/Failed to obtain OAuth token/);
            expect(rejections).toEqual([]);

            await new Promise((resolve) => setTimeout(resolve, 1_500));

            await expect(getToken()).resolves.toMatchObject({ access_token: 'token' });
            expect(rejections).toEqual([]);
        } finally {
            process.off('unhandledRejection', onRejection);
        }
    }, 20_000);
});
