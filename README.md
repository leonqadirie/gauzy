# gauzy - Probabilistic Set Membership Filters for Gleam

[![Package Version](https://img.shields.io/hexpm/v/gauzy)](https://hex.pm/packages/gauzy)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/gauzy/)

Ever need to quickly check if you've seen an item before, without storing *every single item*? That's where `gauzy` comes in!

`gauzy` provides **probabilistic set membership filters** for Gleam. Think of them as super memory-efficient ways to ask
- "Is this item possibly in the set?" or
- "Is this item *definitely* not in the set?".
They're great for large datasets where perfect accuracy isn't strictly needed, but speed and low memory usage are important.

**Currently includes:** Bloom filters and Cuckoo filters (more filter types planned!)

---

## Installation

```sh
gleam add gauzy@2
```

---

Probabilistic data structures are specialized data structures that use randomization to achieve compact representation with controlled error rates. They're particularly useful when:

- Processing large datasets with limited memory
- Approximate answers are acceptable

## Available Data Structures

| | Bloom filter | Cuckoo filter |
| --- | --- | --- |
| False negatives | Never | Never, if you only delete what you inserted |
| Deletion | Not supported | Supported |
| Insertion | Always succeeds | Can fail once nearly full |
| Item count | Estimated | Exact |

### Bloom Filter

A Bloom filter is a space-efficient probabilistic data structure used to test whether an element is a member of a set.

Key properties:
- Zero false negatives (if it says "not in set", it never lies)
- Configurable false positive rate (might say "in set" when it's not)
- Memory efficient for large sets
- Can't delete items - you might need a reset

```gleam
import gauzy/bloom_filter
import murmur3a
import mumu

pub fn main() {
  // Use your own hash functions here. They:
  // - must output an integer hash digest.
  // - should ideally be independent and uniformly distributed
  // - are not required to be cryptographic
  let hash_fn_1 = fn(string_item) {
    murmur3a.hash_string(string_item, 0)
    |> murmur3a.int_digest
  }
  let hash_fn_2 = fn(string_item) {
    mumu.hash(string_item)
  }

  let assert Ok(hash_fn_pair) =
    bloom_filter.new_hash_fn_pair(hash_fn_1, hash_fn_2)

  // Expect to store ~10,000 items, allow 1% error rate
  let assert Ok(filter) = bloom_filter.new(
    capacity: 10_000,
    target_error_rate: 0.01,
    hash_function_pair: hash_fn_pair,
  )

  // Insert items
  let filter = filter
    |> bloom_filter.insert("hello")
    |> bloom_filter.insert("world")

  // Check if items might be in the set
  let in_filter = bloom_filter.might_contain(filter, "hello")  // True
  let also_in_filter = bloom_filter.might_contain(filter, "world")  // True
  let not_in_filter = bloom_filter.might_contain(filter, "goodbye")  // False

  // Get filter properties
  let size = bloom_filter.bit_size(filter)
  let error_rate = bloom_filter.false_positive_rate(filter)
  let hash_count = bloom_filter.hash_fn_count(filter)

  // Estimate how many unique items were inserted. Returns
  // `Error(SaturatedFilter)` when every bit is set, as the estimate diverges.
  let est_cardinality = bloom_filter.estimate_cardinality(filter)

  // Returns an equivalent empty filter
  let empty_filter = bloom_filter.reset(filter)
}
```

### Cuckoo Filter

A Cuckoo filter answers the same question as a Bloom filter, but stores a short *fingerprint* of each item in one of two candidate buckets instead of setting bits. That buys you deletion and an exact item count.

Key properties:
- Zero false negatives — provided you only delete items you actually inserted
- Configurable false positive rate (might say "in set" when it's not)
- Supports deletion, unlike a Bloom filter
- Knows exactly how many items it holds, no estimation needed
- Insertion can fail once the filter is nearly full

```gleam
import gauzy/cuckoo_filter
import murmur3a
import mumu

pub fn main() {
  // Same requirements as for the Bloom filter's hash functions.
  let hash_fn_1 = fn(string_item) {
    murmur3a.hash_string(string_item, 0)
    |> murmur3a.int_digest
  }
  let hash_fn_2 = fn(string_item) {
    mumu.hash(string_item)
  }

  let assert Ok(hash_fn_pair) =
    cuckoo_filter.new_hash_fn_pair(hash_fn_1, hash_fn_2)

  // Expect to store ~10,000 items, allow 1% error rate
  let assert Ok(filter) = cuckoo_filter.new(
    capacity: 10_000,
    target_error_rate: 0.01,
    hash_function_pair: hash_fn_pair,
  )

  // Inserting can run out of room, so it returns a Result. When it does fail
  // the filter you passed in is untouched, and still knows everything it held.
  let assert Ok(filter) = cuckoo_filter.insert(filter, "hello")
  let assert Ok(filter) = cuckoo_filter.insert(filter, "world")

  // Check if items might be in the set
  let in_filter = cuckoo_filter.might_contain(filter, "hello")  // True
  let not_in_filter = cuckoo_filter.might_contain(filter, "goodbye")  // False

  // Remove one previously inserted copy of an item. Deleting an item whose
  // fingerprint is absent fails with ItemNotPresent, so it returns a Result.
  let assert Ok(filter) = cuckoo_filter.delete(filter, "hello")
  let gone = cuckoo_filter.might_contain(filter, "hello")  // False

  // Get filter properties
  let stored = cuckoo_filter.item_count(filter)  // 1, and it's exact
  let room_for = cuckoo_filter.capacity(filter)
  let size = cuckoo_filter.bit_size(filter)
  let error_rate = cuckoo_filter.false_positive_rate(filter)
  let fingerprint_size = cuckoo_filter.fingerprint_bits(filter)

  // Returns an equivalent empty filter
  let empty_filter = cuckoo_filter.reset(filter)
}
```

> **Only delete items you have actually inserted.** A Cuckoo filter matches on fingerprints, so deleting an item that was never inserted but happens to be a false positive drops *another* item's fingerprint — and that item then reads as missing, which is the one thing these filters otherwise rule out. `delete` fails with `ItemNotPresent` when the fingerprint is wholly absent (a double delete or wrong key), but that check cannot catch the false-positive case: an `Ok` means a matching fingerprint was cleared, not that you deleted your own item.

Further documentation can be found at <https://hexdocs.pm/gauzy>.

## Development

```sh
gleam test  # Run the tests
```

---

## Error Handling

All creation operations return a `Result`. Each filter has its own error type — `BloomFilterError` and `CuckooFilterError` — sharing these variants:
- `EqualHashFunctions` —  Hash functions passed are equal.
- `InvalidCapacity` —  Capacity must be positive.
- `InvalidTargetErrorRate` —  Error rate should be in (0.0, 1.0).

`cuckoo_filter.insert` and `cuckoo_filter.insert_many` also return a `Result`:
- `FilterFull` —  No room could be made for the item. The filter is returned to you unchanged.

`cuckoo_filter.delete` also returns a `Result`:
- `ItemNotPresent` —  No slot held the item's fingerprint, so the filter does not contain it.

Check/handle errors using Gleam’s `Result` type.

---

## Contributing

- Run tests with:
  ```sh
  gleam test
  ```
- Please open issues or submit pull requests!

---

## Further Information

- [Hexdocs](https://hexdocs.pm/gauzy/)
- [Bloom filter primer (Wikipedia)](https://en.wikipedia.org/wiki/Bloom_filter)
- [Cuckoo filter primer (Wikipedia)](https://en.wikipedia.org/wiki/Cuckoo_filter)
- [Cuckoo Filter: Practically Better Than Bloom](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf) — the original paper

---
