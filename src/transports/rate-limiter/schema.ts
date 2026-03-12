import * as v from "valibot";
import type { PublicRpcSchema as Base } from "viem";

import type { EIP1193Parameters, ExtendableRpcSignatures, SafelyExtendRpcSchema } from "../../types.js";

const AdditionalParametersSchema = v.tuple([
  v.strictObject({
    __rateLimiter: v.literal(true),
    priority: v.optional(v.number()),
  }),
]);

type AdditionalParameters = v.InferOutput<typeof AdditionalParametersSchema>;

export type RateLimiterSchema = SafelyExtendRpcSchema<
  Base,
  [
    {
      Method: ExtendableRpcSignatures<Base>["Method"];
      AdditionalParameters: AdditionalParameters;
    },
  ]
>;

export function stripAdditionalParameters<Method extends Base[number]["Method"]>(
  args: EIP1193Parameters<RateLimiterSchema, Method>,
): [EIP1193Parameters<Base, Method>, AdditionalParameters | undefined] {
  const x = args.params;

  const len = AdditionalParametersSchema.items.length;

  if (Array.isArray(x) && x.length >= len) {
    const result = v.safeParse(AdditionalParametersSchema, x.slice(-len));

    if (result.success) {
      return [{ ...args, params: x.slice(0, -len) } as EIP1193Parameters<Base, Method>, result.output];
    }
  }

  return [args as EIP1193Parameters<Base, Method>, undefined];
}
