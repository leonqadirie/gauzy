//// Benchmark: `bloom_filter.insert_many` vs. folding `bloom_filter.insert`
//// over the same items, across batch sizes.
////
//// `insert_many` coalesces all bit writes per word into a single update of the
//// immutable `iv` array, whereas folding `insert` updates the array once per
//// bit. This measures whether that coalescing actually pays off, and where.
////
//// Run with: gleam run -m bloom_filter_bench

import gauzy/bloom_filter.{type BloomFilter}
import gleam/int
import gleam/list
import glychee/benchmark
import glychee/configuration
import murmur3a

pub fn main() -> Nil {
  configuration.initialize()
  configuration.set_pair(configuration.Warmup, 1)
  configuration.set_pair(configuration.Time, 3)
  configuration.set_pair(configuration.Parallel, 1)

  benchmark.run(
    [
      benchmark.Function(label: "insert_many", callable: fn(items) {
        // Filter built here (per scenario) is setup, not part of the timing.
        let filter = build_filter(list.length(items))
        fn() { bloom_filter.insert_many(filter, items) }
      }),
      benchmark.Function(label: "fold insert", callable: fn(items) {
        let filter = build_filter(list.length(items))
        fn() { list.fold(items, filter, bloom_filter.insert) }
      }),
    ],
    [
      benchmark.Data(label: "100 items", data: make_items(100)),
      benchmark.Data(label: "10_000 items", data: make_items(10_000)),
      benchmark.Data(label: "100_000 items", data: make_items(100_000)),
    ],
  )
}

/// Builds an empty filter sized for `capacity` items at a 1% error rate.
fn build_filter(capacity: Int) -> BloomFilter(List(Int)) {
  let assert Ok(pair) = bloom_filter.new_hash_fn_pair(make_hash(0), make_hash(1))
  let assert Ok(filter) =
    bloom_filter.new(
      capacity: capacity,
      target_error_rate: 0.01,
      hash_function_pair: pair,
    )
  filter
}

/// A murmur3a-based hash function for `List(Int)` items.
fn make_hash(seed: Int) -> fn(List(Int)) -> Int {
  fn(ints) { ints |> murmur3a.hash_ints(seed) |> murmur3a.int_digest }
}

/// Produces `n` distinct items: `[0], [1], ... [n - 1]`.
fn make_items(n: Int) -> List(List(Int)) {
  int.range(from: 0, to: n, with: [], run: fn(acc, i) { [[i], ..acc] })
}
