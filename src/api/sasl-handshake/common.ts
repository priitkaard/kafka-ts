export type SaslHandshakeRequest = {
    mechanism: string;
};

export type SaslHandshakeResponse = {
    errorCode: number;
    mechanisms: string[];
};
