import gauzy/bloom_filter.{
  type BloomFilter, type BloomFilterError, type HashFunctionPair,
}
import gleam/float
import gleam/int
import gleam/list
import murmur3a

/// Creates a hash function using murmur3a with the given seed.
/// Returns a function that takes a list of integers and produces a hash value.
fn create_hash_function(seed: Int) -> fn(List(Int)) -> Int {
  fn(ints) {
    ints
    |> list.flat_map(to_bytes)
    |> murmur3a.hash_ints(seed)
    |> murmur3a.int_digest
  }
}

/// Spreads an integer over the four bytes `murmur3a.hash_ints` consumes.
///
/// It masks every element of its list down to `0xff`, so handing it integers
/// whole would collapse `[0]` through `[9999]` onto a mere 256 distinct
/// digests, and the tests below would only ever exercise 256 keys.
fn to_bytes(int: Int) -> List(Int) {
  [
    int,
    int.bitwise_shift_right(int, 8),
    int.bitwise_shift_right(int, 16),
    int.bitwise_shift_right(int, 24),
  ]
}

/// Creates a test fixture for a hash function pair.
/// Uses two hash functions with different seeds (0 and 1) for testing purposes.
fn hash_function_pair_fixture() -> HashFunctionPair(List(Int)) {
  let assert Ok(hash_fn_pair) =
    bloom_filter.new_hash_fn_pair(
      create_hash_function(0),
      create_hash_function(1),
    )
  hash_fn_pair
}

/// Creates a test bloom filter with specified capacity and target error rate.
/// Uses the hash function pair fixture for consistent testing.
fn create_test_filter(
  capacity: Int,
  target_err_rate: Float,
) -> BloomFilter(List(Int)) {
  let assert Ok(filter) =
    bloom_filter.new(capacity, target_err_rate, hash_function_pair_fixture())
  filter
}

/// Returns whether all items from `0` (inclusive) to `capacity` (exclusive)
/// are present in the filter.
fn all_items_present(
  in filter: BloomFilter(List(Int)),
  with capacity: Int,
) -> Bool {
  int.range(from: 0, to: capacity, with: True, run: fn(acc, element) {
    acc && bloom_filter.might_contain(filter, [element])
  })
}

/// Returns whether no items from `0` (inclusive) to `capacity` (exclusive)
/// are present in the filter.
fn no_items_present(
  in filter: BloomFilter(List(Int)),
  with capacity: Int,
) -> Bool {
  int.range(from: 0, to: capacity, with: True, run: fn(acc, element) {
    acc && !bloom_filter.might_contain(filter, [element])
  })
}

/// Verifies that a populated filter contains all expected items, has the expected
/// cardinality, rejects unknown items, and clears correctly on reset.
fn verify_filter(filter: BloomFilter(List(Int)), capacity: Int) -> Nil {
  assert all_items_present(in: filter, with: capacity)

  // Estimating is approximate by nature, so allow the count a 1% margin.
  let estimate = bloom_filter.estimate_cardinality(filter)
  assert int.absolute_value(estimate - capacity) <= capacity / 100

  assert !bloom_filter.might_contain(filter, [capacity, capacity])

  let reset_filter = bloom_filter.reset(filter)
  assert no_items_present(in: reset_filter, with: capacity)
  assert bloom_filter.estimate_cardinality(reset_filter) == 0
}

pub fn new_hash_function_pair_test() -> Result(
  HashFunctionPair(List(Int)),
  BloomFilterError,
) {
  let hash_fn_1 = create_hash_function(0)
  let hash_fn_2 = create_hash_function(1)

  let assert Ok(_) = bloom_filter.new_hash_fn_pair(hash_fn_1, hash_fn_2)
  let assert Error(_) = bloom_filter.new_hash_fn_pair(hash_fn_1, hash_fn_1)
}

pub fn new_bloom_filter_test() -> Result(
  BloomFilter(List(Int)),
  BloomFilterError,
) {
  let filter = create_test_filter(100, 0.001)
  assert bloom_filter.bit_size(filter) == 1440
  assert bloom_filter.hash_fn_count(filter) == 10
  // Compared with a tolerance rather than `==`: floating-point results can
  // differ in the last ulp across the Erlang and JavaScript targets.
  assert float.loosely_equals(
    bloom_filter.false_positive_rate(filter),
    0.0009892969942595967,
    tolerating: 1.0e-9,
  )

  let small_filter = create_test_filter(1, 0.1)
  assert bloom_filter.bit_size(small_filter) == 6
  assert bloom_filter.hash_fn_count(small_filter) == 3
  assert float.loosely_equals(
    bloom_filter.false_positive_rate(small_filter),
    0.06091618422799686,
    tolerating: 1.0e-9,
  )

  let hash_fn_pair = hash_function_pair_fixture()
  let assert Error(_) = bloom_filter.new(0, 0.5, hash_fn_pair)
  let assert Error(_) = bloom_filter.new(100, 0.0, hash_fn_pair)
  let assert Error(_) = bloom_filter.new(100, 1.0, hash_fn_pair)
}

pub fn insert_works_test() -> Nil {
  let capacity = 10_000
  let filter = create_test_filter(capacity, 0.001)

  let filter = {
    int.range(from: 0, to: capacity, with: filter, run: fn(bloom, element) {
      bloom_filter.insert(bloom, [element])
    })
  }

  verify_filter(filter, capacity)
}

pub fn false_positive_rate_holds_up_test() -> Nil {
  let capacity = 10_000
  let probes = 50_000
  let filter = create_test_filter(capacity, 0.001)

  let filter =
    int.range(from: 0, to: capacity, with: filter, run: fn(bloom, element) {
      bloom_filter.insert(bloom, [element])
    })

  // Probing keys that were never inserted, the filter should claim only about
  // as many of them as the rate it predicts for itself.
  let false_positives =
    int.range(
      from: capacity,
      to: capacity + probes,
      with: 0,
      run: fn(acc, element) {
        case bloom_filter.might_contain(filter, [element]) {
          True -> acc + 1
          False -> acc
        }
      },
    )

  assert float.loosely_equals(
    int.to_float(false_positives) /. int.to_float(probes),
    bloom_filter.false_positive_rate(filter),
    tolerating: 0.0005,
  )
}

pub fn insert_many_works_test() -> Nil {
  let capacity = 10_000
  let filter = create_test_filter(capacity, 0.001)

  let items = {
    int.range(from: 0, to: capacity, with: [], run: fn(acc, element) {
      [[element], ..acc]
    })
  }

  let filter = bloom_filter.insert_many(filter, items)

  verify_filter(filter, capacity)
}
