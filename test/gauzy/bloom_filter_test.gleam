import gauzy/bloom_filter
import gleam/list
import gleeunit
import murmur3a

pub fn main() {
  gleeunit.main()
}

fn hash_function_pair_fixture() {
  let hash_fn_1 = fn(ints) {
    ints
    |> murmur3a.hash_ints(0)
    |> murmur3a.int_digest
  }

  let hash_fn_2 = fn(ints) {
    ints
    |> murmur3a.hash_ints(1)
    |> murmur3a.int_digest
  }

  let assert Ok(hash_fn_pair) =
    bloom_filter.new_hash_fn_pair(hash_fn_1, hash_fn_2)

  hash_fn_pair
}

pub fn new_hash_function_pair_test() {
  let hash_fn_1 = fn(ints) {
    ints
    |> murmur3a.hash_ints(0)
    |> murmur3a.int_digest
  }

  let hash_fn_2 = fn(ints) {
    ints
    |> murmur3a.hash_ints(1)
    |> murmur3a.int_digest
  }

  let assert Ok(_) = bloom_filter.new_hash_fn_pair(hash_fn_1, hash_fn_2)
  let assert Error(_) = bloom_filter.new_hash_fn_pair(hash_fn_1, hash_fn_1)
}

pub fn new_bloom_filter_test() {
  let assert Ok(filter) =
    bloom_filter.new(100, 0.001, hash_function_pair_fixture())

  assert bloom_filter.bit_size(filter) == 1440
  assert bloom_filter.hash_fn_count(filter) == 10
  assert bloom_filter.false_positive_rate(filter) == 0.0009892969942595967

  let assert Ok(small_filter) =
    bloom_filter.new(1, 0.1, hash_function_pair_fixture())

  assert bloom_filter.bit_size(small_filter) == 6
  assert bloom_filter.hash_fn_count(small_filter) == 3
  assert bloom_filter.false_positive_rate(small_filter) == 0.06091618422799686

  let assert Error(_) = bloom_filter.new(0, 0.5, hash_function_pair_fixture())
  let assert Error(_) = bloom_filter.new(100, 0.0, hash_function_pair_fixture())
  let assert Error(_) = bloom_filter.new(100, 1.0, hash_function_pair_fixture())
}

pub fn insert_works_test() {
  let capacity = 10_000
  let target_err_rate = 0.001

  let assert Ok(filter) =
    bloom_filter.new(capacity, target_err_rate, hash_function_pair_fixture())

  let filter =
    list.range(0, capacity - 1)
    |> list.fold(filter, fn(bloom, element) {
      bloom_filter.insert(bloom, [element])
    })

  assert list.all(list.range(0, capacity - 1), fn(element) {
    bloom_filter.might_contain(filter, [element])
  })

  // As the `HashFunctionPair` is not pairwise independent.
  assert bloom_filter.estimate_cardinality(filter) == 256
  assert !bloom_filter.might_contain(filter, [capacity, capacity])

  let reset_filter = bloom_filter.reset(filter)

  assert !list.all(list.range(0, capacity - 1), fn(element) {
    bloom_filter.might_contain(reset_filter, [element])
  })

  assert bloom_filter.estimate_cardinality(reset_filter) == 0
}

pub fn insert_many_works_test() {
  let capacity = 10_000
  let target_err_rate = 0.001

  let assert Ok(filter) =
    bloom_filter.new(capacity, target_err_rate, hash_function_pair_fixture())

  let items =
    list.range(0, capacity - 1)
    |> list.map(fn(element) { [element] })

  let filter = bloom_filter.insert_many(filter, items)

  assert list.all(list.range(0, capacity - 1), fn(element) {
    bloom_filter.might_contain(filter, [element])
  })

  // As the `HashFunctionPair` is not pairwise independent.
  assert bloom_filter.estimate_cardinality(filter) == 256
  assert !bloom_filter.might_contain(filter, [capacity, capacity])

  let reset_filter = bloom_filter.reset(filter)

  assert !list.all(list.range(0, capacity - 1), fn(element) {
    bloom_filter.might_contain(reset_filter, [element])
  })

  assert bloom_filter.estimate_cardinality(reset_filter) == 0
}
