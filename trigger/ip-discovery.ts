import {  task, schemaTask } from "@trigger.dev/sdk";
import { z } from "zod";

const ipEndpointSchema = z.object({
  ip: z.string(),
});
type IpEndpoint = z.infer<typeof ipEndpointSchema>;

const ipDiscoveryTask = task({
  id: "ip-discovery",
  queue: {
    name: "ip-discovery-task-queue",
  },
  run: async (): Promise<IpEndpoint> => {
    const response = await fetch("https://api.ipify.org?format=json");
    const data = await response.json();
    const { ip } = ipEndpointSchema.parse(data);
    return { ip };
  },
});

const ipDiscoveryWorkflowPayloadSchema = z.object({
  numChildren: z.number().min(1).max(500), // max batch size is 500 per docs
});
type IpDiscoveryWorkflowPayload = z.infer<typeof ipDiscoveryWorkflowPayloadSchema>;

/**
 * This is a sample workflow that demonstrates fan out and aggregation
 */
export const ipDiscoveryWorkflow = schemaTask({
  id: "ip-discovery-workflow",
  schema: ipDiscoveryWorkflowPayloadSchema,
  run: async (payload: IpDiscoveryWorkflowPayload): Promise<string[]> => {
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