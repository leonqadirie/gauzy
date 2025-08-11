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

import gleam/bool
import gleam/dict
import gleam/float
import gleam/int
import gleam/list
import iv.{type Array}

/// The size of an integer within the array
const word_size = 52

/// Represents errors that can occur during Bloom filter operations.
pub type BloomFilterError {
  /// The provided hash functions are equal, which is not allowed.
  EqualHashFunctions
  /// The specified capacity is invalid (must be greater than 0).
  InvalidCapacity
  /// The specified target error rate is invalid (must be between 0.0 and 1.0 exclusively).
  InvalidTargetErrorRate
}

/// A pair of hash functions used by the Bloom filter.
///
/// `item` is the type for which the hash functions provide an `Int` digest.
pub opaque type HashFunctionPair(item) {
  HashFunctionPair(
    /// The first hash function.
    hash_fn_1: fn(item) -> Int,
    /// The second hash function.
    hash_fn_2: fn(item) -> Int,
  )
}

/// Creates a new pair of hash functions for the `BloomFilter`.
///
/// The hash functions must not be equal! For optimal performance,
/// the hash functions should be random, uniform, and pairwise independent.
///
/// * `first_hash_function`: The first hash function.
/// * `second_hash_function`: The second hash function.
pub fn new_hash_fn_pair(hash_fn_1: fn(a) -> Int, hash_fn_2: fn(a) -> Int) {
  case hash_fn_1 == hash_fn_2 {
    False -> Ok(HashFunctionPair(hash_fn_1:, hash_fn_2:))
    True -> Error(EqualHashFunctions)
  }
}

/// A space-efficient data structure to probabilistically check set membership.
pub opaque type BloomFilter(item) {
  BloomFilter(
    /// The underlying bit array.
    array: Array(Int),
    /// The size of the bit array.
    bit_size: Int,
    /// The number of hash functions used.
    hash_fn_count: Int,
    /// The actual false positive rate.
    false_positive_rate: Float,
    /// The pair of hash functions used to generate indices.
    hash_function_pair: HashFunctionPair(item),
    /// The size of index ranges for Kirsch-Mitzenmacher optimization
    chunk_size: Int,
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
  hash_function_pair hash_function_pair: HashFunctionPair(a),
) -> Result(BloomFilter(a), BloomFilterError) {
  use <- bool.guard(capacity < 1, Error(InvalidCapacity))
  use <- bool.guard(
    target_error_rate <=. 0.0 || 1.0 <=. target_error_rate,
    Error(InvalidTargetErrorRate),
  )

  let optimal_bit_size = optimal_bit_size(capacity, target_error_rate)
  let hash_fn_count = optimal_hash_fn_count(optimal_bit_size, capacity)
  let bit_size = case optimal_bit_size % hash_fn_count {
    0 -> optimal_bit_size
    _ -> optimal_bit_size + { hash_fn_count - optimal_bit_size % hash_fn_count }
  }
  let false_positive_rate =
    actual_false_positive_rate(bit_size, capacity, hash_fn_count)
  let chunk_size = bit_size / hash_fn_count
  let word_chunk_count = bit_size / word_size + 1

  Ok(BloomFilter(
    array: iv.repeat(0, word_chunk_count),
    bit_size:,
    false_positive_rate:,
    hash_fn_count:,
    hash_function_pair:,
    chunk_size:,
  ))
}

/// Inserts an item into the `BloomFilter`.
///
/// * `filter`: The `BloomFilter` to insert into.
/// * `item`: The item to insert.
pub fn insert(in filter: BloomFilter(a), insert item: a) -> BloomFilter(a) {
  let indices = get_bit_indices(filter, item)

  let array =
    list.fold(indices, filter.array, fn(array, idx) {
      let word_idx = idx / word_size
      let mask = int.bitwise_shift_left(1, { idx % word_size })
      iv.try_update(array, word_idx, fn(word) { int.bitwise_or(word, mask) })
    })

  BloomFilter(..filter, array:)
}

/// Bulk inserts multiple items into the `BloomFilter`.
/// This is more efficient than inserting items one by one.
///
/// * `filter`: The `BloomFilter` to insert into.
/// * `items`: The list of items to insert.
pub fn insert_many(
  in filter: BloomFilter(a),
  insert items: List(a),
) -> BloomFilter(a) {
  let all_bit_updates =
    list.fold(items, dict.new(), fn(acc, item) {
      let indices = get_bit_indices(filter, item)
      list.fold(indices, acc, fn(word_map, idx) {
        let word_idx = idx / word_size
        let bit_pos = idx % word_size
        case dict.get(word_map, word_idx) {
          Ok(mask) ->
            dict.insert(
              word_map,
              word_idx,
              int.bitwise_or(mask, int.bitwise_shift_left(1, bit_pos)),
            )
          Error(_) ->
            dict.insert(word_map, word_idx, int.bitwise_shift_left(1, bit_pos))
        }
      })
    })

  let array =
    dict.fold(all_bit_updates, filter.array, fn(array, word_idx, mask) {
      iv.try_update(array, word_idx, fn(word) { int.bitwise_or(word, mask) })
    })

  BloomFilter(..filter, array:)
}

/// Checks if the `BloomFilter` might contain the given `item`.
///
/// * `filter`: The `BloomFilter` to check
/// * `item`: The item to check for
pub fn might_contain(in filter: BloomFilter(a), search item: a) -> Bool {
  // let indices =
  get_bit_indices(filter, item)
  |> list.all(fn(idx) {
    let word_idx = idx / word_size
    let word = iv.get_or_default(filter.array, word_idx, 0)
    let mask = int.bitwise_shift_left(1, idx % word_size)

    int.bitwise_and(word, mask) == mask
  })
}

/// Returns the size of the `BloomFilter`'s underlying bit array.
///
/// * `filter`: The `BloomFilter` from which to get the size
pub fn bit_size(of filter: BloomFilter(a)) -> Int {
  filter.bit_size
}

/// Returns the `BloomFilter`'s actual false positive rate
///
/// * `filter`: The `BloomFilter` from which to get the error rate
pub fn false_positive_rate(of filter: BloomFilter(a)) -> Float {
  filter.false_positive_rate
}

/// Returns the number of hash functions the `BloomFilter` uses.
///
/// * `filter`: The `BloomFilter` from which to get the hash function count
pub fn hash_fn_count(of filter: BloomFilter(a)) -> Int {
  filter.hash_fn_count
}

/// Returns an _approximation_ of unique items inserted into the `BloomFilter`.
/// This can differ substantially from reality, especially in smaller filters.
///
/// * `filter`: The `BloomFilter` for which to estimate
pub fn estimate_cardinality(in filter: BloomFilter(a)) -> Int {
  let set_bits =
    iv.fold(filter.array, 0, fn(total_set_bits, word) {
      total_set_bits + count_set_bits(word)
    })

  // Can't panic as m > 0, therefore term > 0
  let assert Ok(partial_calc) =
    float.logarithm(
      1.0 -. int.to_float(set_bits) /. int.to_float(filter.bit_size),
    )

  -1.0
  *. int.to_float(filter.bit_size)
  /. int.to_float(filter.hash_fn_count)
  *. partial_calc
  |> float.round
}

/// Returns an empty `BloomFilter` with the same characteristics as the input filter.
///
/// * `filter`: The `BloomFilter` to reset
pub fn reset(filter filter: BloomFilter(a)) -> BloomFilter(a) {
  BloomFilter(..filter, array: iv.repeat(0, filter.bit_size / word_size + 1))
}

/// Calculates the optimal size in bits of a Bloom filter.
/// Used in filter construction.
///
/// * `capacity`: The number of bits that constitute the filter
/// * `target_err_rate`: The Bloom filter's acceptable false positive rate
fn optimal_bit_size(capacity: Int, target_err_rate: Float) {
  // No panic possible as `2.0` is positive
  let assert Ok(ln_2) = float.logarithm(2.0)
  // No panic possible as `ln_2` is positive
  let assert Ok(ln_2_squared) = float.power(ln_2, 2.0)
  // No panic possible as `target_err_rate` is clearly defined
  let assert Ok(ln_target_err_rate) = float.logarithm(target_err_rate)

  -1.0 *. { int.to_float(capacity) *. ln_target_err_rate } /. ln_2_squared
  |> float.ceiling
  |> float.round
}

/// * `capacity`: The number of elements that the filter shall be able to hold
/// Calculates the optimal number of hash functions for a Bloom filter.
/// Used in filter construction.
///
/// * `bit_size`: The number of bits that constitute the filter
/// * `capacity`: The number of elements that the filter shall be able to hold
fn optimal_hash_fn_count(bit_size: Int, capacity: Int) {
  // No panic possible as `float.logarithm(2.0)` is clearly defined
  let assert Ok(ln_2) = float.logarithm(2.0)
  int.to_float(bit_size) /. int.to_float(capacity) *. ln_2
  |> float.round
  |> int.max(1)
}

/// Calculates the actual false positive rate of a `Bloomfilter`.
/// Used in filter construction.
///
/// * `bit_size`: The number of bits that constitute the filter
/// * `capacity`: The number of elements that the filter shall be able to hold
/// * `hash_fns_count`: The number of hash functions the filter uses
///
/// Returns an `f64` as the expected false positive rate.
fn actual_false_positive_rate(
  bit_size: Int,
  capacity: Int,
  hash_fn_count: Int,
) -> Float {
  // Can't panic in `float.power` as:
  // - the result of `float.exponential` is always positive
  // - `hash_fn_count` is always positive
  let assert Ok(false_positive_rate) =
    float.power(
      1.0
        -. float.exponential(
        -1.0
        *. { int.to_float(hash_fn_count) *. int.to_float(capacity) }
        /. { int.to_float(bit_size) },
      ),
      int.to_float(hash_fn_count),
    )

  false_positive_rate
}

/// Returns a list of unique, sorted bit indices for the given `item`
/// using the `BloomFilter`'s hash functions.
///
/// * `bloom_filter`: The `BloomFilter` to get the bit indices from
/// * `item`: The item to calculate the bit indices for
fn get_bit_indices(filter: BloomFilter(a), item: a) -> List(Int) {
  let HashFunctionPair(hash_fn_1:, hash_fn_2:) = filter.hash_function_pair

  let hash_1 = case hash_fn_1(item) {
    hash_1 if hash_1 < 0 -> { 2 * hash_1 } |> int.absolute_value
    hash_1 -> hash_1
  }
  let hash_2 = case hash_fn_2(item) {
    hash_2 if hash_2 < 0 -> { 2 * hash_2 } |> int.absolute_value
    hash_2 -> hash_2
  }

  list.range(0, filter.hash_fn_count - 1)
  |> list.map(fn(i) {
    i * filter.chunk_size + { hash_1 + i * hash_2 } % filter.chunk_size
  })
}

/// Counts the number of set bits (population count) in an integer.
/// Used by `estimate_cardinality`.
///
/// * `word`: The integer (representing a word from the bit array) for which to count set bits.
fn count_set_bits(word: Int) -> Int {
  use acc, i <- list.fold(list.range(0, word_size - 1), 0)
  let mask = int.bitwise_shift_left(1, i)
  case int.bitwise_and(word, mask) == mask {
    True -> acc + 1
    False -> acc
  }
}
