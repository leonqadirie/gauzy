import gauzy/bloom_filter
import gleam/list
import gleeunit
import gleeunit/should
import murmur3a

pub fn main() {
  gleeunit.main()
}

fn hash_function_pair_fixture() {
  let hash_fn_1 = fn(ints) {
    murmur3a.hash_ints(ints, 0) |> murmur3a.int_digest
  }
  let hash_fn_2 = fn(ints) {
    murmur3a.hash_ints(ints, 1) |> murmur3a.int_digest
  }
  let assert Ok(hash_fn_pair) =
    bloom_filter.new_hash_fn_pair(hash_fn_1, hash_fn_2)
  hash_fn_pair
}

pub fn new_hash_function_pair_test() {
  let hash_fn_1 = fn(ints) {
    murmur3a.hash_ints(ints, 0) |> murmur3a.int_digest
  }
  let hash_fn_2 = fn(ints) {
    murmur3a.hash_ints(ints, 1) |> murmur3a.int_digest
  }

  bloom_filter.new_hash_fn_pair(hash_fn_1, hash_fn_2)
  |> should.be_ok

  bloom_filter.new_hash_fn_pair(hash_fn_1, hash_fn_1)
  |> should.be_error
}

pub fn new_bloom_filter_test() {
  let assert Ok(filter) =
    bloom_filter.new(100, 0.001, hash_function_pair_fixture())

  bloom_filter.hash_fn_count(filter)
  |> should.equal(10)

  bloom_filter.bit_size(filter) |> should.equal(1440)

  bloom_filter.false_positive_rate(filter)
  |> should.equal(0.0009892969942595967)
  let assert Ok(small_filter) =
    bloom_filter.new(1, 0.1, hash_function_pair_fixture())

  bloom_filter.hash_fn_count(small_filter)
  |> should.equal(3)

  bloom_filter.bit_size(small_filter) |> should.equal(6)

  bloom_filter.false_positive_rate(small_filter)
  |> should.equal(0.06091618422799686)

  bloom_filter.new(0, 0.5, hash_function_pair_fixture()) |> should.be_error
  bloom_filter.new(100, 0.0, hash_function_pair_fixture()) |> should.be_error
  bloom_filter.new(100, 1.0, hash_function_pair_fixture()) |> should.be_error
}

pub fn it_works_test() {
  let capacity = 10_000
  let target_err_rate = 0.001
  let assert Ok(filter) =
    bloom_filter.new(capacity, target_err_rate, hash_function_pair_fixture())

  let assert Ok(filter) =
    list.range(0, capacity - 1)
    |> list.try_fold(filter, fn(bloom, element) {
      bloom_filter.try_insert(bloom, [element])
    })

  list.range(0, capacity - 1)
  |> list.all(fn(element) { bloom_filter.might_contain(filter, [element]) })
  |> should.be_true

  // As the `HashFunctionPair` is not pairwise independent.
  bloom_filter.estimate_cardinality(filter)
  |> should.equal(256)

  bloom_filter.might_contain(filter, [capacity, capacity])
  |> should.be_false

  let reset_filter = bloom_filter.reset(filter)

  list.range(0, capacity - 1)
  |> list.all(fn(element) {
    bloom_filter.might_contain(reset_filter, [element])
  })
  |> should.be_false

  bloom_filter.estimate_cardinality(reset_filter)
  |> should.equal(0)
}
