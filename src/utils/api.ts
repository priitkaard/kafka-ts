import { Decoder } from './decoder';
import { Encoder } from './encoder';

export type Api<Request, Response> = {
    apiKey: number;
    apiVersion: number;
    request: (encoder: Encoder, body: Request) => Encoder;
    response: (buffer: Decoder) => Promise<Response> | Response;
};

export const createApi = <Request, Response>(api: Api<Request, Response>) => api;
