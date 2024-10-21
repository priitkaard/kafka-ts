import { describe, expect, it } from 'vitest';
import { distributeMessagesToTopicPartitionLeaders } from './messages-to-topic-partition-leaders';

describe('Distribute messages to partition leader ids', () => {
    describe('distributeMessagesToTopicPartitionLeaders', () => {
        it('snoke', () => {
            const result = distributeMessagesToTopicPartitionLeaders(
                [{ topic: 'topic', partition: 0, key: null, value: null, offset: 0n, timestamp: 0n, headers: {} }],
                { topic: { 0: 1 } },
            );
            expect(result).toMatchInlineSnapshot(`
              {
                "1": {
                  "topic": {
                    "0": [
                      {
                        "headers": {},
                        "key": null,
                        "offset": 0n,
                        "partition": 0,
                        "timestamp": 0n,
                        "topic": "topic",
                        "value": null,
                      },
                    ],
                  },
                },
              }
            `);
        });
    });
});
