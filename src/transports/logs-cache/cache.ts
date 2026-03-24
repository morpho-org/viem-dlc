import type { Cache, Store } from "../../types.js";
import { createKeyedMutex } from "../../utils/with-keyed-mutex.js";

type Shard<T> = Record<string, T>;

function splitKey(key: string, separator: string): { group: string; subkey: string } {
  const [group, subkey] = key.split(separator, 2);
  if (group === undefined || subkey === undefined) {
    throw new Error(`[ShardedCache] Key '${key}' lacked separator '${separator}'`);
  }

  return { group, subkey };
}

/**
 * A cache that groups related keys for efficient batch reads.
 *
 * Keys must contain a separator (e.g., `"filterPart+rangePart"`) that divides them into
 * a group prefix and a subkey. All entries sharing the same group prefix are stored together.
 *
 * **Assumptions**
 * - Keys contain exactly one separator character
 * - Entries within a group are frequently read/written together
 *
 * @example
 * const cache = new ShardedCache(
 *   store,
 *   JSON.stringify,
 *   JSON.parse,
 *   '+',           // separator
 *   1_000_000      // max shard size in bytes
 * )
 *
 * // Keys with same prefix are grouped: "0xabc...+0:9999" and "0xabc...+10000:19999"
 * await cache.write([
 *   { key: 'filterA+0:9999', value: chunk1 },
 *   { key: 'filterA+10000:19999', value: chunk2 }
 * ])
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export class ShardedCache<T extends {}> implements Cache<T> {
  private withKeyedMutex = createKeyedMutex().withKeyedMutex;

  constructor(
    private readonly store: Store,
    private readonly stringify: (value: Shard<T>) => string,
    private readonly parse: (value: string) => Shard<T>,
    private readonly separator: string,
  ) {}

  private async getGroup(group: string, subkeys: string[]): Promise<(T | undefined)[]> {
    if (subkeys.length === 0) return [];

    const mapRaw = await this.store.get(group);
    const map = mapRaw !== null ? this.parse(mapRaw) : {};

    return subkeys.map((subkey) => map[subkey]);
  }

  private async setGroup(group: string, items: { subkey: string; value: T }[]): Promise<void> {
    if (items.length === 0) return;

    const mapRaw = await this.store.get(group);
    const map = mapRaw !== null ? this.parse(mapRaw) : {};

    for (const item of items) {
      map[item.subkey] = item.value;
    }

    await this.store.set(group, this.stringify(map));
  }

  async read(keys: string[]): Promise<(T | undefined)[]> {
    // Group by the first element of each key, and provide reverse lookup to original array order.
    // Values include the subkey (remaining elements of key) and its index in the `keys` array.
    const targetsByGroup = new Map<string, { subkey: string; idx: number }[]>();

    keys.forEach((key, i) => {
      const { group: groupName, subkey } = splitKey(key, this.separator);

      const targets = targetsByGroup.get(groupName) ?? [];
      targets.push({ subkey, idx: i });
      targetsByGroup.set(groupName, targets);
    });

    // Preallocate results array
    const results: (T | undefined)[] = new Array(keys.length);

    // Read each group and fill results by index
    await Promise.all(
      Array.from(targetsByGroup.entries()).map(async ([groupName, targets]) => {
        const values = await this.getGroup(
          groupName,
          targets.map((target) => target.subkey),
        );

        targets.forEach((target, i) => {
          results[target.idx] = values[i];
        });
      }),
    );

    return results;
  }

  async write(items: { key: string; value: T }[]): Promise<void> {
    // Assign each item to a group keyed by the first element of its key.
    // Values include the subkey (remaining elements of key) and original value.
    const groups = new Map<string, { subkey: string; value: T }[]>();
    for (const { key, value } of items) {
      const { group: groupName, subkey } = splitKey(key, this.separator);

      if (!groups.has(groupName)) {
        groups.set(groupName, []);
      }
      groups.get(groupName)!.push({ subkey, value });
    }

    // Write each group (with per-group locking to prevent concurrent read-modify-write races)
    await Promise.all(
      Array.from(groups.entries()).map(([groupName, items]) =>
        this.withKeyedMutex(groupName, () => this.setGroup(groupName, items)),
      ),
    );
  }
}
