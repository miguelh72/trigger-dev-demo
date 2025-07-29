import { queue, schemaTask } from "@trigger.dev/sdk";
import { z } from "zod";

const fibonacciWorkflowQueue = queue({
  name: "fibonacci-workflow",
});

const fibonacciTaskQueue = queue({
  name: "fibonacci-task",
});

const payloadSchema = z.object({
  n: z.number().min(1),
});
type Payload = z.infer<typeof payloadSchema>;

/**
 * This is a sample workflow that demonstrates idempotency and recursive workflows
 */
export const fibonacciTask = schemaTask({
  id: "fibonacci-idempotent-workflow",
  schema: payloadSchema,
  queue: fibonacciWorkflowQueue,
  maxDuration: 300, // Stop executing after 300 secs (5 mins) of compute
  run: async (payload: Payload): Promise<{ result: number }> => {
    // Base cases
    if (payload.n === 1) {
      return {
        result: 0,
      };
    }
    if (payload.n === 2) {
      return {
        result: 1,
      };
    }

    // else recursively trigger the task

    // calling and waiting for the n-2 task to finish first since the n-1 task will trigger the n-2 task cache
    const handleN2 = await fibonacciTask.triggerAndWait(
      {
        n: payload.n - 2,
      },
      {
        idempotencyKey: `fibonacci-${payload.n - 2}`,
        queue: fibonacciTaskQueue.name,
      }
    );
    const handleN1 = await fibonacciTask.triggerAndWait(
      {
        n: payload.n - 1,
      },
      {
        idempotencyKey: `fibonacci-${payload.n - 1}`,
        queue: fibonacciTaskQueue.name,
      }
    );

    if (handleN1.ok && handleN2.ok) {
      return {
        result: handleN1.output.result + handleN2.output.result,
      };
    }

    if (!handleN1.ok) {
      console.log(handleN1.error);
    }
    if (!handleN2.ok) {
      console.log(handleN2.error);
    }
    throw new Error("Failed to calculate fibonacci");
  },
});
