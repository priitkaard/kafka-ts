import { describe, expect, it } from "vitest";
import { distributeAssignmentsToNodesBalanced, distributeAssignmentsToNodesOptimized } from "./assignments-to-replicas";

describe("Distribute assignments to replica ids", () => {
    describe("distributeAssignmentsToNodesBalanced", () => {
        it("smoke", () => {
            const result = distributeAssignmentsToNodesBalanced({ topic: [0, 1] }, { topic: { 0: [0, 1], 1: [1, 2] } });
            expect(result).toMatchInlineSnapshot(`
              {
                "1": {
                  "topic": [
                    0,
                  ],
                },
                "2": {
                  "topic": [
                    1,
                  ],
                },
              }
            `);
        });
    });

    describe("distributeAssignmentsToNodesOptimized", () => {
        it("smoke", () => {
            const result = distributeAssignmentsToNodesOptimized(
                { topic: [0, 1] },
                { topic: { 0: [0, 1], 1: [1, 2] } },
            );
            expect(result).toMatchInlineSnapshot(`
              {
                "1": {
                  "topic": [
                    0,
                    1,
                  ],
                },
              }
            `);
        });
    });
});
