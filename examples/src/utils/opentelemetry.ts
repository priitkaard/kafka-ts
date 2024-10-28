import { context, ROOT_CONTEXT, trace } from '@opentelemetry/api';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks';
import { JaegerExporter } from '@opentelemetry/exporter-jaeger/build/src/jaeger';
import { NodeSDK } from '@opentelemetry/sdk-node';
import { BatchSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { Tracer } from 'kafka-ts';

const contextManager = new AsyncHooksContextManager();
contextManager.enable();
context.setGlobalContextManager(contextManager);

const exporter = new JaegerExporter({ port: 6832, host: 'localhost', maxPacketSize: 9000 });

const sdk = new NodeSDK({
    serviceName: 'kafka-ts',
    traceExporter: exporter,
    spanProcessors: [new BatchSpanProcessor(exporter, { maxQueueSize: 128, maxExportBatchSize: 128 })],
    instrumentations: [getNodeAutoInstrumentations()],
});

sdk.start();

const tracer = trace.getTracer('kafka-ts');

export class OpenTelemetryTracer implements Tracer {
    startActiveSpan(module, method, { body, ...metadata } = {} as any, callback) {
        return tracer.startActiveSpan(
            `${module}.${method} ${metadata?.message ?? ''}`,
            { attributes: metadata },
            metadata?.root ? ROOT_CONTEXT : context.active(),
            (span) => {
                const result = callback();
                if (result instanceof Promise) {
                    return result.finally(() => span.end());
                }
                span.end();
                return result;
            },
        );
    }
}
