# gauzy - Probabilistic Data Structures for Gleam

[![Package Version](https://img.shields.io/hexpm/v/gauzy)](https://hex.pm/packages/gauzy)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/gauzy/)

gauzy is a Gleam library providing flexible implementations of probabilistic data structures. These data structures offer space-efficient alternatives to traditional collections with controllable accuracy trade-offs.

## Installation

```sh
gleam add gauzy@1
```

## Overview

Probabilistic data structures are specialized data structures that use randomization to achieve compact representation with controlled error rates. They're particularly useful when:

- Processing massive datasets with limited memory
- Approximate answers are acceptable
- Perfect accuracy isn't required but fast performance is critical

## Available Data Structures

### Bloom Filter

A Bloom filter is a space-efficient probabilistic data structure used to test whether an element is a member of a set. It can tell you:

- "The element is definitely not in the set" (100% accurate)
- "The element is probably in the set" (with a controllable false positive rate)

Key properties:
- Zero false negatives (if it says "not in set", it never lies)
- Configurable false positive rate (might say "in set" when it's not)
- Memory efficient for large sets

```gleam
import gauzy/bloom_filter
import murmur3a

pub fn main() {
  // Use your own hash functions here. They must output an integer hash digest.
  let hash_fn_1 = fn(string_item) {
    murmur3a.hash_string(string_item, 0) |> murmur3a.int_digest
  }
  let hash_fn_2 = fn(string_item) {
    murmur3a.hash_string(string_item, 1) |> murmur3a.int_digest
  }
  let assert Ok(hash_fn_pair) =
    bloom_filter.new_hash_fn_pair(hash_fn_1, hash_fn_2)

  // Create a new Bloom filter that can hold ~10,000 items while maintaining a 1% false positive rate
  let assert Ok(filter) = bloom_filter.new(
    capacity: 10_000,
    target_error_rate: 0.01,
    with_hashes: hash_pair,
  )

  // Insert items
  let assert Ok(filter) = bloom_filter.try_insert(filter, "hello")
  let assert Ok(filter) = bloom_filter.try_insert(filter, "world")

  // Check if items might be in the set
  let in_filter = bloom_filter.might_contain(filter, "hello")  // True
  let also_in_filter = bloom_filter.might_contain(filter, "world")  // True
  let not_in_filter = bloom_filter.might_contain(filter, "goodbye")  // False

  // Get filter properties
  let size = bloom_filter.bit_size(filter)
  let error_rate = bloom_filter.error_rate(filter)
  let hash_count = bloom_filter.hash_fn_count(filter)

  // Reset the filter to empty state
  let empty_filter = bloom_filter.reset(filter)
}
```

Further documentation can be found at <https://hexdocs.pm/gauzy>.

## Development

```sh
gleam test  # Run the tests
```
