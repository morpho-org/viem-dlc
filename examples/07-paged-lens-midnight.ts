/**
 * `policy({ paged: true })`: a lens that reports how far it got instead of being bisected into.
 *
 * The lens measures Midnight borrower health — debt against oracle-priced borrow capacity, the
 * way `isHealthy()` does — for a batch of `(market, borrower)` pairs. It stops when gas runs low
 * (the transport retries the tail) and declines pairs whose market was never created, which
 * `toMarket` reverts on. Those land in `skipped`, rebased onto the caller's input. A partial result
 * is a successful response; check `skipped` yourself.
 *
 * Candidates come from Morpho's liquidation-candidates API, an over-inclusive feed that itself says
 * to re-read every pair on-chain before acting — which is exactly what the lens does.
 */
import { policy } from "@morpho-org/viem-dlc/actions";
import { deployless } from "@morpho-org/viem-dlc/transports";
import { sol } from "soltag";
import { type Address, createPublicClient, getAbiItem, getAddress, type Hex, http, keccak256, toHex } from "viem";
import { readContract } from "viem/actions";
import { base } from "viem/chains";

const rpcUrl = process.env.RPC_URL;
if (!rpcUrl) throw new Error("Set RPC_URL (see examples/README.md)");

const MIDNIGHT = "0xAdedD8ab6dE832766Fedf0FaC4992E5C4D3EA18A" as const;
const CANDIDATES_URL = "https://api.morpho.org/markets/midnight/liquidation-candidates";

type Candidate = { market_id: Hex; borrower: Address; health_factor: number };

/** Every candidate at or below `healthFactorLte` (matured positions are always included). */
async function fetchCandidates(healthFactorLte: number): Promise<Candidate[]> {
  const out: Candidate[] = [];
  let cursor: string | null = null;
  do {
    const url = new URL(CANDIDATES_URL);
    url.searchParams.set("chain_id", String(base.id));
    url.searchParams.set("health_factor_lte", String(healthFactorLte));
    url.searchParams.set("limit", "100");
    if (cursor) url.searchParams.set("cursor", cursor);
    const res = await fetch(url);
    if (!res.ok) throw new Error(`${url.pathname}: HTTP ${res.status}`);
    const body = (await res.json()) as { cursor?: string | null; data: Candidate[] };
    out.push(...body.data.map((c) => ({ ...c, borrower: getAddress(c.borrower) })));
    cursor = body.cursor || null;
  } while (cursor);
  return out;
}

const IMidnight = `
  struct CollateralParams { address token; uint256 lltv; uint256 liquidationCursor; address oracle; }
  struct Market {
    uint256 chainId; address midnight; address loanToken; CollateralParams[] collateralParams;
    uint256 maturity; uint256 rcfThreshold; address enterGate; address liquidatorGate;
  }
  interface IMidnight {
    function toMarket(bytes32 id) external view returns (Market memory);
    function debt(bytes32 id, address user) external view returns (uint128);
    function collateralBitmap(bytes32 id, address user) external view returns (uint128);
    function collateral(bytes32 id, address user, uint256 index) external view returns (uint128);
    function liquidationLocked(bytes32 id, address user) external view returns (bool);
  }
  interface IOracle { function price() external view returns (uint256); }
`;

// Index order, single pass, element 0 always attempted; stop on low gas (retryable tail), skip on
// a condition that will decline identically next time (the market does not exist). Only \`toMarket\`
// is wrapped in try/catch — a broken oracle reverts the frame rather than masquerading as a skip.
const healthLens = sol("MidnightHealthPagedLens")`
  pragma solidity ^0.8.24;
  ${IMidnight}
  contract MidnightHealthPagedLens {
    uint256 constant WAD = 1e18;
    uint256 constant ORACLE_PRICE_SCALE = 1e36;
    // Measured on Base via eth_estimateGas: a served element costs ~48k (up to ~56k with two
    // collateral legs), a skipped one ~12k. The reserve covers the assembly epilogue and return.
    uint256 constant PER_ELEMENT_ESTIMATE = 70_000;
    uint256 constant RETURN_RESERVE = 30_000;

    IMidnight immutable midnight;
    constructor(IMidnight _midnight) { midnight = _midnight; }

    struct Input { bytes32 id; address borrower; }
    struct Health {
      uint128 debt;
      uint128 maxDebt;   // borrow capacity: sum over activated collaterals of value * lltv
      bool locked;
      bool matured;
    }

    function page(Input[] calldata inputs)
      external view returns (Health[] memory results, uint256[] memory skipped)
    {
      results = new Health[](inputs.length);
      skipped = new uint256[](inputs.length);
      uint256 nResults;
      uint256 nSkipped;
      for (uint256 i = 0; i < inputs.length; i++) {
        if (i > 0 && gasleft() < RETURN_RESERVE + PER_ELEMENT_ESTIMATE) break;
        try midnight.toMarket(inputs[i].id) returns (Market memory market) {
          results[nResults++] = _health(market, inputs[i].id, inputs[i].borrower);
        } catch {
          skipped[nSkipped++] = i;
        }
      }
      assembly {
        mstore(results, nResults)
        mstore(skipped, nSkipped)
      }
    }

    // Mirrors isHealthy(): maxDebt accumulates collateral * price / SCALE * lltv / WAD over the
    // activated slots (bit j of the bitmap set iff slot j holds a non-zero balance).
    function _health(Market memory market, bytes32 id, address borrower) private view returns (Health memory h) {
      h.debt = midnight.debt(id, borrower);
      h.locked = midnight.liquidationLocked(id, borrower);
      h.matured = block.timestamp > market.maturity;
      uint128 bitmap = midnight.collateralBitmap(id, borrower);
      uint256 maxDebt;
      for (uint256 j = 0; j < market.collateralParams.length; j++) {
        if ((bitmap & (uint128(1) << j)) == 0) continue;
        CollateralParams memory cp = market.collateralParams[j];
        uint256 value = uint256(midnight.collateral(id, borrower, j)) * IOracle(cp.oracle).price() / ORACLE_PRICE_SCALE;
        maxDebt += value * cp.lltv / WAD;
      }
      h.maxDebt = uint128(maxDebt);
    }
  }
`;

const client = createPublicClient({
  chain: base,
  transport: deployless(http(rpcUrl), { gasLimit: 50_000_000 }),
});

const candidates = await fetchCandidates(1.5);
console.log(`${candidates.length} candidate (market, borrower) pairs from the API`);

// Plant pairs on a market that was never created, so `skipped` is non-empty on purpose.
const bogusMarket = keccak256(toHex("not a Midnight market"));
const inputs = candidates.flatMap(({ market_id: id, borrower }, i) =>
  i % 10 === 0
    ? [
        { id: bogusMarket, borrower },
        { id, borrower },
      ]
    : [{ id, borrower }],
);

const [results, skipped] = await readContract(client, {
  ...healthLens.with(MIDNIGHT),
  functionName: "page",
  args: [inputs],
  stateOverride: [
    policy({
      abi: getAbiItem({ abi: healthLens.abi, name: "page" }),
      paged: true,
      // eth_estimateGas on Base fits ~818k + 48.5k·N for this lens (the constant is mostly the
      // counterfactual deploy). Padded ~25%; an over-packed chunk costs one more round trip, not a
      // bisection, so the model only needs to be a sane opening guess.
      batch: { gas: { constant: 850_000, linear: 60_000, quadratic: 0 } },
    }),
  ],
});

console.log(`${inputs.length} inputs → ${results.length} results, ${skipped.length} skipped`);
console.log(`every skipped input is the bogus market: ${skipped.every((i) => inputs[Number(i)]?.id === bogusMarket)}`);

// liquidate() admits a position when debt > 0 && !locked && (matured || debt > maxDebt); the
// liquidator gate (market.liquidatorGate.canLiquidate(caller)) is caller-specific and left out here.
const borrowers = results.filter((h) => h.debt > 0n);
const liquidatable = borrowers.filter((h) => !h.locked && (h.matured || h.debt > h.maxDebt));
const healthFactor = (h: (typeof results)[number]) => Number(h.maxDebt) / Number(h.debt);

const matured = liquidatable.filter((h) => h.matured).length;
console.log(
  `\n${borrowers.length} borrowers with debt, ${liquidatable.length} liquidatable (${matured} past maturity, ${liquidatable.length - matured} unhealthy)`,
);
console.log("lowest health factors (maxDebt / debt):");
for (const h of [...borrowers].sort((a, b) => healthFactor(a) - healthFactor(b)).slice(0, 5)) {
  const flags = [h.matured && "matured", h.locked && "locked", h.debt > h.maxDebt && "unhealthy"].filter(Boolean);
  console.log(`  ${healthFactor(h).toFixed(4).padStart(9)}  debt ${h.debt}  ${flags.join(" ")}`);
}
