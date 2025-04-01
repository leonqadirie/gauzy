//// This module provides an implementation of a Bloom filter, a space-efficient
//// probabilistic data structure that is used to test whether an element is a
//// member of a set. False positive matches are possible, but false negatives
//// are not – in other words, a query returns either "possibly in set" or
//// "definitely not in set".
////
//// Bloom filters are useful in situations where the size of the set would
//// require an impractically large amount of memory to store, or where the
//// cost of a false positive is acceptable compared to the cost of a more
//// precise data structure.
////
//// The module provides functions for creating, inserting into, querying, and
//// resetting Bloom filters.
//// Optimization step size for finding optimal Bloom filter parameters.

import gleam/bool
import gleam/float
import gleam/int
import gleam/list
import iv.{type Array}

const optimization_step = 1.01

/// Represents errors that can occur during Bloom filter operations.
pub type BloomFilterError {
  /// The provided hash functions are equal, which is not allowed.
  EqualHashFunctions
  /// An error occurred during insertion, likely an out-of-bounds index.
  InsertionError
  /// The specified capacity is invalid (must be greater than 0).
  InvalidCapacity
  /// The specified target error rate is invalid (must be between 0.0 and 1.0 exclusively).
  InvalidTargetErrorRate
}

/// A pair of hash functions used by the Bloom filter.
///
/// `a` is the type of item that the hash functions operate on.
pub opaque type HashFunctionPair(a) {
  HashFunctionPair(
    /// The first hash function.
    hash_fn_1: fn(a) -> Int,
    /// The second hash function.
    hash_fn_2: fn(a) -> Int,
  )
}

/// Creates a new pair of hash functions for the `BloomFilter`.
///
/// The hash functions must not be equal!  For optimal performance,
/// the hash functions should be random, uniform, and independent.
///
/// * `first_hash_function`: The first hash function.
/// * `second_hash_function`: The second hash function.
pub fn new_hash_fn_pair(hash_fn_1: fn(a) -> Int, hash_fn_2: fn(a) -> Int) {
  case hash_fn_1 == hash_fn_2 {
    False -> Ok(HashFunctionPair(hash_fn_1:, hash_fn_2:))
    True -> Error(EqualHashFunctions)
  }
}

/// A Bloom filter data structure.
///
/// `a` is the type of item that can be stored in the filter.
pub opaque type BloomFilter(a) {
  BloomFilter(
    /// The underlying bit array.
    array: Array(Int),
    /// The size of the bit array.
    bit_size: Int,
    /// The actual false positive rate.
    error_rate: Float,
    /// The number of hash functions used.
    hash_fn_count: Int,
    /// The pair of hash functions used to generate indices.
    hash_function_pair: HashFunctionPair(a),
  )
}

/// Creates a new `BloomFilter`.
///
/// * `capacity`: The number of items the `BloomFilter` is expected to hold.
/// * `target_error_rate`: The desired false positive rate (between 0.0 and 1.0).
/// * `hash_function_pair`: The hash functions used to generate indices.
pub fn new(
  capacity capacity: Int,
  target_error_rate target_error_rate: Float,
  with_hashes hash_function_pair: HashFunctionPair(a),
) -> Result(BloomFilter(a), BloomFilterError) {
  use <- bool.guard(capacity < 1, Error(InvalidCapacity))
  use <- bool.guard(
    target_error_rate <=. 0.0 || 1.0 <=. target_error_rate,
    Error(InvalidTargetErrorRate),
  )

  let #(bit_size, hash_fn_count, error_rate) =
    optimize(capacity, target_error_rate)

  Ok(BloomFilter(
    array: iv.repeat(0, bit_size),
    bit_size:,
    error_rate:,
    hash_fn_count:,
    hash_function_pair:,
  ))
}

/// Tries to insert an item into the `BloomFilter`.
///
/// * `filter`: The `BloomFilter` to insert into.
/// * `item`: The item to insert.
pub fn try_insert(
  filter: BloomFilter(a),
  item: a,
) -> Result(BloomFilter(a), BloomFilterError) {
  let indices = get_bit_indices(filter, item)

  let array =
    list.try_fold(indices, filter.array, fn(array, idx) {
      iv.set(array, idx, 1)
    })
  case array {
    Ok(array) -> Ok(BloomFilter(..filter, array:))
    Error(_err) -> Error(InsertionError)
  }
}

/// Checks if the `BloomFilter` might contain the given `item`.
///
/// * `filter`: The `BloomFilter` to check
/// * `item`: The item to check for
pub fn might_contain(filter: BloomFilter(a), item: a) -> Bool {
  get_bit_indices(filter, item)
  |> list.all(fn(idx) {
    case iv.get(filter.array, idx) {
      Ok(1) -> True
      _ -> False
    }
  })
}

/// Returns the size of the `BloomFilter`'s underlying bit array.
///
/// * `filter`: The `BloomFilter` to get the size from
pub fn bit_size(filter filter: BloomFilter(a)) -> Int {
  filter.bit_size
}

/// Returns the `BloomFilter`'s actual false positive rate
///
/// * `filter`: The `BloomFilter` to get the error rate from.
pub fn error_rate(filter filter: BloomFilter(a)) -> Float {
  filter.error_rate
}

/// Returns the number of hash functions the `BloomFilter` uses.
///
/// * `filter`: The `BloomFilter` to get the hash function count from
pub fn hash_fn_count(filter filter: BloomFilter(a)) -> Int {
  filter.hash_fn_count
}

/// Returns an empty `BloomFilter` with the same characteristics as the input filter.
///
/// * `filter`: The `BloomFilter` to reset
pub fn reset(filter: BloomFilter(a)) -> BloomFilter(a) {
  let BloomFilter(
    _filter,
    bit_size:,
    error_rate:,
    hash_fn_count:,
    hash_function_pair:,
  ) = filter

  BloomFilter(
    array: iv.repeat(0, bit_size),
    bit_size:,
    error_rate:,
    hash_fn_count:,
    hash_function_pair:,
  )
}

/// Proxy function that relays the input to the recursive function `optimize_values`.
/// Used in Bloom filter construction.
///
/// * `capacity`: Intended elements the Bloom filter shall be able to hold
/// * `target_err_rate`: The Bloom filter's acceptable false positive rate
///
/// Returns *approximately* optimal (num_bits, hash_fn_count, error_rate).
fn optimize(capacity: Int, target_error_rate: Float) -> #(Int, Int, Float) {
  let #(num_bits, hash_fn_count, error_rate) =
    optimize_values(
      int.to_float(capacity),
      4.0 *. int.to_float(capacity),
      2.0,
      target_error_rate,
    )

  #(
    num_bits |> float.floor |> float.round,
    hash_fn_count |> float.floor |> float.round,
    error_rate,
  )
}

/// Recursive function to *approximate* optimal Bloom filter properties.
/// Evaluates filter properties for the input parameters and optimizes them if needed.
/// Used in Bloom filter construction.
///
/// * `capacity`: Intended elements the Bloom filter shall be able to hold
/// * `num_bits`: The number of bits that constitute the filter
/// * `hash_fns_count`: The number of hash functions the filter uses
/// * `target_err_rate`: The Bloom filter's acceptable false positive rate
fn optimize_values(
  capacity: Float,
  num_bits: Float,
  hash_fns_count: Float,
  target_error_rate: Float,
) -> #(Float, Float, Float) {
  let error_rate = false_positive_rate(num_bits, capacity, hash_fns_count)

  let is_acceptable_error_rate = error_rate <. target_error_rate
  case is_acceptable_error_rate {
    False ->
      optimize_values(
        capacity,
        float.ceiling(num_bits *. optimization_step),
        optimal_hash_fn_count(
          float.ceiling(num_bits *. optimization_step),
          capacity,
        ),
        target_error_rate,
      )
    True -> #(num_bits, float.ceiling(hash_fns_count), error_rate)
  }
}

/// Calculates the false positive rate of a Bloom filter with the properties of the parameters.
/// Used in filter construction.
///
/// * `bits`: The number of bits that constitute the filter
/// * `capacity`: The number of elements that the filter shall be able to hold
/// * `hash_fns_count`: The number of hash functions the filter uses
///
/// Returns an `f64` as the expected false positive rate.
fn false_positive_rate(
  bits: Float,
  capacity: Float,
  hash_fn_count: Float,
) -> Float {
  let assert Ok(false_positive_rate) =
    1.0
    -. float.exponential(
      -1.0 *. hash_fn_count *. { capacity +. 0.5 } /. { bits -. 1.0 },
    )
    |> float.power(hash_fn_count)

  false_positive_rate
}

/// Calculates the optimal number of hash functions for a Bloom filter.
/// Used in filter construction.
///
/// * `bits`: The number of bits that constitute the filter
/// * `capacity`: The number of elements that the filter shall be able to hold
fn optimal_hash_fn_count(bits: Float, capacity: Float) -> Float {
  let assert Ok(ln_2) = float.logarithm(2.0)
  bits /. capacity *. ln_2
}

/// Returns a list of unique, sorted bit indices for the given `item`
/// using the `BloomFilter`'s hash functions.
///
/// * `bloom_filter`: The `BloomFilter` to get the bit indices from
/// * `item`: The item to calculate the bit indices for
fn get_bit_indices(bloom_filter: BloomFilter(a), item: a) -> List(Int) {
  let BloomFilter(
    _error_rate,
    _array,
    bit_size:,
    hash_fn_count:,
    hash_function_pair:,
  ) = bloom_filter

  let HashFunctionPair(hash_fn_1:, hash_fn_2:) = hash_function_pair

  let hash_1 = case hash_fn_1(item) {
    hash_1 if hash_1 < 0 -> { 2 * hash_1 } |> int.absolute_value
    hash_1 -> hash_1
  }
  let hash_2 = case hash_fn_2(item) {
    hash_2 if hash_2 < 0 -> { 2 * hash_2 } |> int.absolute_value
    hash_2 -> hash_2
  }

  list.range(0, hash_fn_count - 1)
  |> list.map(fn(i) { { hash_1 + i * hash_2 } % bit_size })
  |> list.unique
  |> list.sort(int.compare)
}
