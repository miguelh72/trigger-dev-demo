import { task, schemaTask } from "@trigger.dev/sdk";
import { z } from "zod";

const ipEndpointSchema = z.object({
  ip: z.string(),
});
type IpEndpoint = z.infer<typeof ipEndpointSchema>;

const ipDiscoveryTask = task({
  id: "ip-discovery",
  queue: {
    name: "ip-discovery-task",
  },
  run: async (): Promise<IpEndpoint> => {
    const response = await fetch("https://api.ipify.org?format=json");
    const data = await response.json();
    const { ip } = ipEndpointSchema.parse(data);
    return { ip };
  },
});

const ipDiscoveryBatchTaskPayloadSchema = z.object({
  numChildren: z.number().min(1).max(500),
});
type IpDiscoveryBatchTaskPayload = z.infer<
  typeof ipDiscoveryBatchTaskPayloadSchema
>;

const ipDiscoveryBatchTask = schemaTask({
  id: "ip-discovery-batch",
  queue: {
    name: "ip-discovery-batch-task",
  },
  schema: ipDiscoveryBatchTaskPayloadSchema,
  run: async (payload: IpDiscoveryBatchTaskPayload): Promise<string[]> => {
    const { numChildren } = payload;

    // Fan out the ip discovery tasks
    const results = await ipDiscoveryTask.batchTriggerAndWait(
      Array.from({ length: numChildren }, (_) => ({
        payload: undefined,
      }))
    );

    // Aggregate the list of unique IPs
    const successfulRuns = results.runs.filter((result) => result.ok);
    const uniqueIps = new Set(successfulRuns.map((result) => result.output.ip));
    return Array.from(uniqueIps);
  },
});

const ipDiscoveryWorkflowPayloadSchema = z.object({
  // The max batch size is 500 per docs, but we will delegate batches of up to 500 to a batch task
  // and batch call the batch tasks. Therefore, we can handle a max of 500 * 500 = 250,000 concurrent
  // ip discovery tasks
  numChildren: z.number().min(1).max(250_000),
});
type IpDiscoveryWorkflowPayload = z.infer<
  typeof ipDiscoveryWorkflowPayloadSchema
>;

/**
 * This is a sample workflow that demonstrates fan out and aggregation
 */
export const ipDiscoveryWorkflow = schemaTask({
  id: "ip-discovery-workflow",
  schema: ipDiscoveryWorkflowPayloadSchema,
  run: async (payload: IpDiscoveryWorkflowPayload): Promise<string[]> => {
    const { numChildren } = payload;

    // Determine how many batch tasks we need of up to 500 children each
    const batchTaskPayloads = Array.from(
      { length: Math.ceil(numChildren / 500) },
      (_, i) => ({
        numChildren: Math.min(500, numChildren - i * 500),
      })
    );

    // Batch trigger the batch tasks
    const batchTaskResults = await ipDiscoveryBatchTask.batchTriggerAndWait(
      batchTaskPayloads.map((payload) => ({
        payload,
      }))
    );

    // Aggregate the list of unique IPs
    const successfulRuns = batchTaskResults.runs.filter((result) => result.ok);
    const uniqueIps = new Set(
      successfulRuns.flatMap((result) => result.output)
    );
    return Array.from(uniqueIps);
  },
});
