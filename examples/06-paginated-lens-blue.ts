/**
 * A paginated lens: the envelope calls the lens's per-item function once per element in its own
 * frame, so the page reports how far it got instead of being bisected into, and an element that
 * runs out of gas is reported in-band rather than killing the page.
 *
 * The lens measures Morpho Blue borrower health on Robinhood Chain — accrued debt against
 * oracle-priced borrow capacity, the way `_isHealthy()` does — for a batch of `(market, borrower)`
 * pairs. The envelope stops when gas runs low (the transport retries the tail); the per-item
 * function declines pairs whose market was never created. Those land in `skipped`, rebased onto
 * the caller's input. A partial result is a successful response; check `skipped` yourself.
 *
 * Candidates come from Morpho's GraphQL API, ordered by health factor. The API is a coverage source,
 * never a correctness dependency: every pair is re-read on-chain by the lens.
 *
 * This one is delivered by state override rather than as initcode, which is what to reach for when
 * bytes bind well before gas — here EIP-3860 caps a chunk at ~700 pairs while the frame would pay
 * for a few thousand. The wide event says when it is worth it: `(gas_limit_observed - fixed_gas) /
 * item_gas_avg` far above `elements_requested / nominal_batches`.
 */
import { readLens } from "@morpho-org/viem-dlc/actions";
import { deployless } from "@morpho-org/viem-dlc/transports";
import { sol } from "soltag";
import { type Address, createPublicClient, getAddress, type Hex, http, keccak256, toHex } from "viem";
import { robinhood } from "viem/chains";

const rpcUrl = process.env.ROBINHOOD_RPC_URL ?? robinhood.rpcUrls.default.http[0];

const MORPHO = "0x9D53d5E3bd5E8d4Cbfa6DB1ca238AEA02E651010" as const;
const GRAPHQL_URL = "https://api.morpho.org/graphql";

type Candidate = { id: Hex; borrower: Address; healthFactor: number };

/** Positions on listed markets at or below `healthFactorLte`, worst first. */
async function fetchCandidates(healthFactorLte: number, first = 1_000): Promise<Candidate[]> {
  const out: Candidate[] = [];
  for (let skip = 0; ; skip += first) {
    const res = await fetch(GRAPHQL_URL, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        query: `query ($chainIds: [Int!], $hfLte: Float!, $first: Int!, $skip: Int!) {
          marketPositions(
            where: { chainId_in: $chainIds, healthFactor_lte: $hfLte, marketListed: true }
            orderBy: HealthFactor, orderDirection: Asc, first: $first, skip: $skip
          ) { items { healthFactor market { marketId } user { address } } }
        }`,
        variables: { chainIds: [robinhood.id], hfLte: healthFactorLte, first, skip },
      }),
    });
    if (!res.ok) throw new Error(`GraphQL: HTTP ${res.status}`);
    const body = (await res.json()) as {
      data?: {
        marketPositions: { items: { healthFactor: number; market: { marketId: Hex }; user: { address: string } }[] };
      };
      errors?: { message: string }[];
    };
    if (body.errors?.length) throw new Error(`GraphQL: ${body.errors[0]?.message}`);
    const items = body.data?.marketPositions.items ?? [];
    for (const item of items) {
      out.push({ id: item.market.marketId, borrower: getAddress(item.user.address), healthFactor: item.healthFactor });
    }
    if (items.length < first) return out;
  }
}

const IMorpho = `
  struct MarketParams { address loanToken; address collateralToken; address oracle; address irm; uint256 lltv; }
  struct Market {
    uint128 totalSupplyAssets; uint128 totalSupplyShares; uint128 totalBorrowAssets;
    uint128 totalBorrowShares; uint128 lastUpdate; uint128 fee;
  }
  struct Position { uint256 supplyShares; uint128 borrowShares; uint128 collateral; }
  interface IMorpho {
    function idToMarketParams(bytes32 id) external view returns (MarketParams memory);
    function market(bytes32 id) external view returns (Market memory);
    function position(bytes32 id, address user) external view returns (Position memory);
  }
  interface IIrm {
    function borrowRateView(MarketParams memory marketParams, Market memory market) external view returns (uint256);
  }
  interface IOracle { function price() external view returns (uint256); }
`;

// The per-item function reverts on a condition that will decline identically next time (the
// market was never created); the envelope records that as a skip. Oracle and IRM reverts revert
// the item too — the envelope cannot tell a deliberate decline from a broken dependency, so keep
// per-item reverts to conditions that are genuinely permanent.
const healthLens = sol("BlueHealthLens")`
  pragma solidity ^0.8.24;
  ${IMorpho}
  contract BlueHealthLens {
    uint256 constant WAD = 1e18;
    uint256 constant ORACLE_PRICE_SCALE = 1e36;
    uint256 constant VIRTUAL_SHARES = 1e6;
    uint256 constant VIRTUAL_ASSETS = 1;

    IMorpho immutable morpho;
    constructor(IMorpho _morpho) { morpho = _morpho; }

    struct Input { bytes32 id; address borrower; }
    struct Health {
      uint256 borrowed;  // debt in loan assets after interest accrual (rounded up, as the contract does)
      uint256 maxBorrow; // collateral * price / SCALE * lltv / WAD
    }

    function healthOf(Input calldata x) external view returns (Health memory) {
      Market memory m = morpho.market(x.id);
      require(m.lastUpdate != 0);
      return _health(x.id, m, x.borrower);
    }

    // Mirrors _isHealthy(): accrue interest since lastUpdate the way _accrueInterest() does
    // (3-term Taylor of rate * elapsed), convert borrow shares to assets rounding up, and compare
    // against collateral value scaled by lltv.
    function _health(bytes32 id, Market memory m, address borrower) private view returns (Health memory h) {
      MarketParams memory p = morpho.idToMarketParams(id);
      Position memory pos = morpho.position(id, borrower);

      uint256 totalBorrowAssets = m.totalBorrowAssets;
      uint256 elapsed = block.timestamp - m.lastUpdate;
      if (elapsed != 0 && totalBorrowAssets != 0 && p.irm != address(0)) {
        uint256 x = IIrm(p.irm).borrowRateView(p, m) * elapsed;
        uint256 second = x * x / (2 * WAD);
        uint256 third = second * x / (3 * WAD);
        totalBorrowAssets += totalBorrowAssets * (x + second + third) / WAD;
      }

      uint256 shares = pos.borrowShares;
      h.borrowed = (shares * (totalBorrowAssets + VIRTUAL_ASSETS) + (m.totalBorrowShares + VIRTUAL_SHARES - 1))
        / (m.totalBorrowShares + VIRTUAL_SHARES);
      h.maxBorrow = uint256(pos.collateral) * IOracle(p.oracle).price() / ORACLE_PRICE_SCALE * p.lltv / WAD;
    }
  }
`;

const client = createPublicClient({
  chain: robinhood,
  // What the chain's public endpoint grants, as `gas_limit_observed` reported it — a figure of the
  // provider, not the chain: a paid endpoint for this same chain answers with 600M. It only has to
  // be close, and low is the safe side: it sizes the opening wave, every later chunk is packed from
  // what the pages measured, and understating it costs a round trip rather than a result.
  transport: deployless(http(rpcUrl), { gasLimit: 50_000_000 }),
});

const candidates = await fetchCandidates(1.5);
console.log(`${candidates.length} candidate (market, borrower) pairs from the API`);

// Plant pairs on a market that was never created, so `skipped` is non-empty on purpose.
const bogusMarket = keccak256(toHex("not a Morpho Blue market"));
const inputs = candidates.flatMap(({ id, borrower }, i) =>
  i % 10 === 0
    ? [
        { id: bogusMarket, borrower },
        { id, borrower },
      ]
    : [{ id, borrower }],
);

const { results, skipped } = await readLens(client, {
  ...healthLens.with(MORPHO),
  functionName: "healthOf",
  args: inputs,
  // `override` places the envelope at a fixed address through `eth_call`'s state-override
  // parameter instead of creating it from initcode, so no byte cap applies and the frame's gas is
  // the only bound — ~2 300 pairs a frame against that 50M cap, where the byte cap allowed ~700. A provider
  // that ignores overrides is detected on the opening wave and the request finishes as initcode,
  // counted in `override_fallbacks_unsupported`. `gas` is this lens's own cost, copied from
  // `fixed_gas`, `item_gas_avg` and `item_gas_stddev`; it is a property of the lens, so the same
  // three numbers serve every provider and chain.
  batch: { envelope: "override", gas: { fixed: 565_000, item: { avg: 22_000, stddev: 8_400 } } },
});

console.log(`${inputs.length} inputs → ${results.length} results, ${skipped.length} skipped`);
const planted = skipped.filter((i) => inputs[i]?.id === bogusMarket).length;
console.log(`of the skips, ${planted} are the planted bogus market`);

// Blue liquidates any unhealthy position: borrowed > maxBorrow.
const borrowers = results.filter((h) => h.borrowed > 0n);
const liquidatable = borrowers.filter((h) => h.borrowed > h.maxBorrow);
const healthFactor = (h: (typeof results)[number]) => Number(h.maxBorrow) / Number(h.borrowed);

console.log(`\n${borrowers.length} borrowers with debt, ${liquidatable.length} liquidatable`);
console.log("lowest health factors (maxBorrow / borrowed), with the API's figure alongside:");
// Only planted rows are skipped, so `results` lines up with `candidates`. Dust positions are
// excluded from the ranking: below ~1000 base units the contract's integer rounding makes the
// ratio meaningless (a 32-unit debt reads exactly 1.0 while the API's unrounded figure is ~1.07).
const ranked = results.map((h, i) => ({ h, api: candidates[i]?.healthFactor })).filter(({ h }) => h.borrowed >= 1_000n);
for (const { h, api } of ranked.sort((a, b) => healthFactor(a.h) - healthFactor(b.h)).slice(0, 5)) {
  console.log(
    `  on-chain ${healthFactor(h).toFixed(4).padStart(8)}   api ${api?.toFixed(4) ?? "?"}   borrowed ${h.borrowed}`,
  );
}
