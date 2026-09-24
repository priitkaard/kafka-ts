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

    it('does not hot-loop when the token response omits expires_in', async () => {
        let attempts = 0;
        const endpoint = await startTokenEndpoint(() => {
            attempts++;
            return { status: 200, body: { access_token: 'token', refresh_token: 'refresh' } };
        });

        const getToken = oAuthAuthenticator({ endpoint, clientId: 'id', clientSecret: 'secret' });
        await expect(getToken()).resolves.toMatchObject({ access_token: 'token' });

        await new Promise((resolve) => setTimeout(resolve, 2_500));

        expect(attempts).toBe(1);
    }, 20_000);

    it('does not hot-loop when expires_in is null or non-positive', async () => {
        for (const expiresIn of [null, 0, -1]) {
            let attempts = 0;
            const endpoint = await startTokenEndpoint(() => {
                attempts++;
                return {
                    status: 200,
                    body: { access_token: 'token', refresh_token: 'refresh', expires_in: expiresIn },
                };
            });

            const getToken = oAuthAuthenticator({ endpoint, clientId: 'id', clientSecret: 'secret' });
            await expect(getToken()).resolves.toMatchObject({ access_token: 'token' });

            await new Promise((resolve) => setTimeout(resolve, 2_500));

            expect(attempts).toBe(1);

            await new Promise((resolve) => server!.close(resolve));
            server = undefined;
        }
    }, 30_000);

    it('honours an expires_in sent as a string', async () => {
        let attempts = 0;
        const endpoint = await startTokenEndpoint(() => {
            attempts++;
            return {
                status: 200,
                body: { access_token: 'token', refresh_token: 'refresh', expires_in: '3600' },
            };
        });

        const getToken = oAuthAuthenticator({ endpoint, clientId: 'id', clientSecret: 'secret' });
        await expect(getToken()).resolves.toMatchObject({ access_token: 'token' });

        await new Promise((resolve) => setTimeout(resolve, 1_500));

        expect(attempts).toBe(1);
    }, 20_000);

    it('keeps serving the current token while a refresh fails', async () => {
        let issued = 0;
        const endpoint = await startTokenEndpoint(() => {
            issued++;
            return issued === 1
                ? { status: 200, body: { access_token: 'first', refresh_token: 'refresh', expires_in: 4 } }
                : { status: 503 };
        });

        const getToken = oAuthAuthenticator({ endpoint, clientId: 'id', clientSecret: 'secret' });
        await expect(getToken()).resolves.toMatchObject({ access_token: 'first' });

        await new Promise((resolve) => setTimeout(resolve, 3_000));

        await expect(getToken()).resolves.toMatchObject({ access_token: 'first' });
        expect(issued).toBeGreaterThan(1);
    }, 30_000);

    it('stops serving a token that expired while the refresh kept failing', async () => {
        let issued = 0;
        const grants: (string | undefined)[] = [];
        server = http.createServer((req, res) => {
            let body = '';
            req.on('data', (chunk) => (body += chunk));
            req.on('end', () => {
                issued++;
                grants.push(new URLSearchParams(body).get('grant_type') ?? undefined);
                if (issued === 1) {
                    res.writeHead(200, { 'Content-Type': 'application/json' });
                    return res.end(JSON.stringify({ access_token: 'first', refresh_token: 'refresh', expires_in: 2 }));
                }
                res.writeHead(503, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ error: 'unavailable' }));
            });
        });
        await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', resolve));
        const endpoint = `http://127.0.0.1:${(server!.address() as AddressInfo).port}/token`;

        const getToken = oAuthAuthenticator({ endpoint, clientId: 'id', clientSecret: 'secret' });
        await expect(getToken()).resolves.toMatchObject({ access_token: 'first' });

        await new Promise((resolve) => setTimeout(resolve, 6_500));

        await expect(getToken()).rejects.toThrow(/Failed to obtain OAuth token/);
        expect(grants.slice(1)).toContain('client_credentials');
    }, 30_000);

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
