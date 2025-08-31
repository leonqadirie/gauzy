import gauzy/bloom_filter
import gleam/list
import gleeunit
import murmur3a

pub fn main() {
  gleeunit.main()
}

/// Creates a hash function using murmur3a with the given seed.
/// Returns a function that takes a list of integers and produces a hash value.
fn create_hash_function(seed: Int) {
  fn(ints) {
    ints
    |> murmur3a.hash_ints(seed)
    |> murmur3a.int_digest
  }
}

/// Creates a test fixture for a hash function pair.
/// Uses two hash functions with different seeds (0 and 1) for testing purposes.
fn hash_function_pair_fixture() {
  let assert Ok(hash_fn_pair) =
    bloom_filter.new_hash_fn_pair(
      create_hash_function(0),
      create_hash_function(1),
    )
  hash_fn_pair
}

/// Creates a test bloom filter with specified capacity and target error rate.
/// Uses the hash function pair fixture for consistent testing.
fn create_test_filter(capacity: Int, target_err_rate: Float) {
  let assert Ok(filter) =
    bloom_filter.new(capacity, target_err_rate, hash_function_pair_fixture())
  filter
}

/// Verifies that all items from `0` to `capacity-1` are present in the filter.
/// Asserts that might_contain returns true for all expected elements.
fn verify_all_items_present(filter, capacity: Int) {
  assert list.all(list.range(0, capacity - 1), fn(element) {
    bloom_filter.might_contain(filter, [element])
  })
}

/// Verifies that resetting a filter clears all elements.
/// Checks that the reset filter doesn't contain previous elements and has cardinality `0`.
fn verify_reset_filter(filter, capacity: Int) {
  let reset_filter = bloom_filter.reset(filter)

  assert !list.all(list.range(0, capacity - 1), fn(element) {
    bloom_filter.might_contain(reset_filter, [element])
  })

  assert bloom_filter.estimate_cardinality(reset_filter) == 0
}

pub fn new_hash_function_pair_test() {
  let hash_fn_1 = create_hash_function(0)
  let hash_fn_2 = create_hash_function(1)

  let assert Ok(_) = bloom_filter.new_hash_fn_pair(hash_fn_1, hash_fn_2)
  let assert Error(_) = bloom_filter.new_hash_fn_pair(hash_fn_1, hash_fn_1)
}

pub fn new_bloom_filter_test() {
  let filter = create_test_filter(100, 0.001)
  assert bloom_filter.bit_size(filter) == 1440
  assert bloom_filter.hash_fn_count(filter) == 10
  assert bloom_filter.false_positive_rate(filter) == 0.0009892969942595967

  let small_filter = create_test_filter(1, 0.1)
  assert bloom_filter.bit_size(small_filter) == 6
  assert bloom_filter.hash_fn_count(small_filter) == 3
  assert bloom_filter.false_positive_rate(small_filter) == 0.06091618422799686

  let hash_fn_pair = hash_function_pair_fixture()
  let assert Error(_) = bloom_filter.new(0, 0.5, hash_fn_pair)
  let assert Error(_) = bloom_filter.new(100, 0.0, hash_fn_pair)
  let assert Error(_) = bloom_filter.new(100, 1.0, hash_fn_pair)
}

pub fn insert_works_test() {
  let capacity = 10_000
  let filter = create_test_filter(capacity, 0.001)

  let filter =
    list.range(0, capacity - 1)
    |> list.fold(filter, fn(bloom, element) {
      bloom_filter.insert(bloom, [element])
    })

  verify_all_items_present(filter, capacity)

  // As the `HashFunctionPair` is not pairwise independent.
  assert bloom_filter.estimate_cardinality(filter) == 256
  assert !bloom_filter.might_contain(filter, [capacity, capacity])

  verify_reset_filter(filter, capacity)
}

pub fn insert_many_works_test() {
  let capacity = 10_000
  let filter = create_test_filter(capacity, 0.001)

  let items =
    list.range(0, capacity - 1)
    |> list.map(fn(element) { [element] })

  let filter = bloom_filter.insert_many(filter, items)

  verify_all_items_present(filter, capacity)

  // As the `HashFunctionPair` is not pairwise independent.
  assert bloom_filter.estimate_cardinality(filter) == 256
  assert !bloom_filter.might_contain(filter, [capacity, capacity])

  verify_reset_filter(filter, capacity)
}
