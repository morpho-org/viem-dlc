/**
 * `policy({ paged: true })`: a lens that reports how far it got instead of being bisected into.
 *
 * The lens measures Morpho Blue borrower health on Robinhood Chain — accrued debt against
 * oracle-priced borrow capacity, the way `_isHealthy()` does — for a batch of `(market, borrower)`
 * pairs. It stops when gas runs low (the transport retries the tail) and declines pairs whose
 * market was never created. Those land in `skipped`, rebased onto the caller's input. A partial
 * result is a successful response; check `skipped` yourself.
 *
 * Candidates come from Morpho's GraphQL API, ordered by health factor. The API is a coverage source,
 * never a correctness dependency: every pair is re-read on-chain by the lens.
 */
import { MAX_INITCODE_SIZE, policy } from "@morpho-org/viem-dlc/actions";
import { deployless } from "@morpho-org/viem-dlc/transports";
import { sol } from "soltag";
import { type Address, createPublicClient, getAbiItem, getAddress, type Hex, http, keccak256, toHex } from "viem";
import { readContract } from "viem/actions";
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

// Index order, single pass, element 0 always attempted; stop on low gas (retryable tail), skip on
// a condition that will decline identically next time (the market was never created). Oracle and
// IRM reverts fail the frame rather than masquerading as skips.
const healthLens = sol("BlueHealthPagedLens")`
  pragma solidity ^0.8.24;
  ${IMorpho}
  contract BlueHealthPagedLens {
    uint256 constant WAD = 1e18;
    uint256 constant ORACLE_PRICE_SCALE = 1e36;
    uint256 constant VIRTUAL_SHARES = 1e6;
    uint256 constant VIRTUAL_ASSETS = 1;
    // Measured on Robinhood Chain via eth_estimateGas: a served element costs ~36k (up to ~68k
    // when it is the first touch of its market's storage), a skipped one ~10k.
    uint256 constant PER_ELEMENT_ESTIMATE = 80_000;
    uint256 constant RETURN_RESERVE = 30_000;

    IMorpho immutable morpho;
    constructor(IMorpho _morpho) { morpho = _morpho; }

    struct Input { bytes32 id; address borrower; }
    struct Health {
      uint256 borrowed;  // debt in loan assets after interest accrual (rounded up, as the contract does)
      uint256 maxBorrow; // collateral * price / SCALE * lltv / WAD
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
        Market memory m = morpho.market(inputs[i].id);
        if (m.lastUpdate == 0) { skipped[nSkipped++] = i; continue; }
        results[nResults++] = _health(inputs[i].id, m, inputs[i].borrower);
      }
      assembly {
        mstore(results, nResults)
        mstore(skipped, nSkipped)
      }
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

const [results, skipped] = await readContract(client, {
  ...healthLens.with(MORPHO),
  functionName: "page",
  args: [inputs],
  stateOverride: [
    policy({
      abi: getAbiItem({ abi: healthLens.abi, name: "page" }),
      paged: true,
      // eth_estimateGas on Robinhood Chain fits ~853k + 36k·N for this lens (the constant is mostly
      // the counterfactual deploy); padded ~25%. An over-packed chunk costs one more round trip, not
      // a bisection, so the model only needs to be a sane opening guess. Uncompressed, the 49,152-byte
      // initcode cap binds first (~704 borrowers/call at 64 B each); `compress` shrinks the ABI-padded
      // input ~9× on the wire, so gas binds instead — ~1,700 fully served in one call at this cap,
      // with a hard wrapper-phase OOG (not a paged stop) just past it, which the padding keeps clear of.
      batch: { batchSize: MAX_INITCODE_SIZE, compress: true, gas: { constant: 900_000, linear: 45_000, quadratic: 0 } },
    }),
  ],
});

console.log(`${inputs.length} inputs → ${results.length} results, ${skipped.length} skipped`);
console.log(`every skipped input is the bogus market: ${skipped.every((i) => inputs[Number(i)]?.id === bogusMarket)}`);

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
