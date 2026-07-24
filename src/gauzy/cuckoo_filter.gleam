//// This module provides an implementation of a Cuckoo filter, a space-efficient
//// probabilistic data structure that is used to test whether an element is a
//// member of a set. False positive matches are possible, but false negatives
//// are not – in other words, a query returns either "possibly in set" or
//// "definitely not in set".
////
//// Unlike a Bloom filter, a Cuckoo filter supports deletion and knows exactly
//// how many items it holds. In exchange, insertion can fail: once the filter
//// approaches its load limit there may be no room left for a new item.
////
//// A Cuckoo filter stores a short *fingerprint* of each item in one of two
//// candidate buckets. When both are full an existing fingerprint is evicted to
//// its own alternate bucket, which may evict another, and so on – the cuckoo
//// behaviour the structure is named after. Because the filter is immutable a
//// failed insertion simply discards the half-finished eviction chain, so the
//// filter you already hold is never damaged by a failed insert.
////
//// The module provides functions for creating, inserting into, querying,
//// deleting from, and resetting Cuckoo filters.

import gleam/bool
import gleam/float
import gleam/int
import gleam/list
import gleam/result
import iv.{type Array}

/// The number of bits packed into each array word.
///
/// Deliberately capped well below the BEAM's ~60-bit fixnum width: on the
/// JavaScript target ints are IEEE-754 doubles, exact only up to 2^53. Keeping a
/// word under 2^52 leaves margin so a fully-set word is always an exact `Number`.
/// (Gleam's JS bitwise ops go through `BigInt`, so the ops themselves are
/// correct, but the resulting word is still *stored* as a double.) A larger word
/// would pack more densely on Erlang but silently lose low bits on JS.
const word_size = 52

/// The number of fingerprint slots per bucket.
///
/// Four is the standard choice: it sustains a load factor of ~95% before
/// insertions start to fail, where two slots manage ~84% and eight buy
/// only ~98% at twice the lookup cost.
const bucket_size = 4

/// The load factor `new` sizes the filter for. See `bucket_size`.
const max_load_factor = 0.95

/// How many fingerprints an insertion may relocate before giving up.
const max_kicks = 500

/// The fingerprint value marking a slot as unoccupied.
/// Real fingerprints start at 1, so it can never be one of them.
const empty_slot = 0

/// Mask of the low 32 bits, the width `scramble` mixes at.
const u32_mask = 4_294_967_295

/// The eviction PRNG's starting state: 2^32 divided by the golden ratio.
/// Any non-zero value works; xorshift is a no-op on a zero state.
const initial_seed = 2_654_435_769

/// Represents errors that can occur during Cuckoo filter operations.
pub type CuckooFilterError {
  /// The provided hash functions are equal, which is not allowed.
  EqualHashFunctions
  /// The specified capacity is invalid (must be greater than 0).
  InvalidCapacity
  /// The specified target error rate is invalid (must be between 0.0 and 1.0 exclusively).
  InvalidTargetErrorRate
  /// The filter could not house the item after `max_kicks` relocations.
  FilterFull
  /// A `delete` found no slot holding the item's fingerprint in either
  /// candidate bucket, so the filter does not contain it.
  ///
  /// Because membership is matched by fingerprint, this is the *only* thing
  /// `delete` can detect: a *missing* fingerprint is certain, but a *matching*
  /// one is not proof you deleted your own item — see `delete`.
  ItemNotPresent
}

/// A pair of hash functions used by the Cuckoo filter.
///
/// `item` is the type for which the hash functions provide an `Int` digest.
pub opaque type HashFunctionPair(item) {
  HashFunctionPair(
    /// The hash function selecting an item's first candidate bucket.
    hash_fn_1: fn(item) -> Int,
    /// The hash function producing an item's fingerprint.
    hash_fn_2: fn(item) -> Int,
  )
}

/// Creates a new pair of hash functions for the `CuckooFilter`.
///
/// The hash functions must not be equal! For optimal performance,
/// the hash functions should be random, uniform, and pairwise independent.
///
/// * `first_hash_function`: The first hash function.
/// * `second_hash_function`: The second hash function.
pub fn new_hash_fn_pair(
  hash_fn_1: fn(a) -> Int,
  hash_fn_2: fn(a) -> Int,
) -> Result(HashFunctionPair(a), CuckooFilterError) {
  use <- bool.guard(hash_fn_1 == hash_fn_2, Error(EqualHashFunctions))
  Ok(HashFunctionPair(hash_fn_1:, hash_fn_2:))
}

/// A space-efficient data structure to probabilistically check set membership,
/// with support for deletion.
pub opaque type CuckooFilter(item) {
  CuckooFilter(
    /// The fingerprint slots, `slots_per_word` of them packed into each word.
    array: Array(Int),
    /// The number of buckets. Always a power of two, so that a bucket's
    /// partner can be found by XOR-ing in the mixed fingerprint.
    bucket_count: Int,
    /// The number of bits each stored fingerprint occupies.
    fingerprint_bits: Int,
    /// The largest storable fingerprint, and the mask covering one slot.
    fingerprint_mask: Int,
    /// The number of fingerprints packed into a single word.
    slots_per_word: Int,
    /// The upper bound on the false positive rate.
    false_positive_rate: Float,
    /// The number of fingerprints currently stored.
    item_count: Int,
    /// The state of the PRNG picking which fingerprint to evict.
    seed: Int,
    /// The pair of hash functions used to generate buckets and fingerprints.
    hash_function_pair: HashFunctionPair(item),
  )
}

/// Creates a new `CuckooFilter`.
///
/// The filter is sized to hold at least `capacity` items, and usually more:
/// bucket counts are rounded up to a power of two.
///
/// Fingerprints are capped at `word_size` bits, so error rates below roughly
/// `2.0e-15` are not achievable. `false_positive_rate` reports what was.
///
/// * `capacity`: The number of items the `CuckooFilter` is expected to hold.
/// * `target_error_rate`: The desired false positive rate (between 0.0 and 1.0).
/// * `hash_function_pair`: The hash functions used to generate buckets and fingerprints.
pub fn new(
  capacity capacity: Int,
  target_error_rate target_error_rate: Float,
  hash_function_pair hash_function_pair: HashFunctionPair(a),
) -> Result(CuckooFilter(a), CuckooFilterError) {
  use <- bool.guard(capacity < 1, Error(InvalidCapacity))
  use <- bool.guard(
    target_error_rate <=. 0.0 || 1.0 <=. target_error_rate,
    Error(InvalidTargetErrorRate),
  )

  let fingerprint_bits = optimal_fingerprint_bits(target_error_rate)
  let bucket_count = optimal_bucket_count(capacity)
  let slots_per_word = word_size / fingerprint_bits

  Ok(CuckooFilter(
    array: iv.repeat(0, word_count(bucket_count, slots_per_word)),
    bucket_count:,
    fingerprint_bits:,
    fingerprint_mask: int.bitwise_shift_left(1, fingerprint_bits) - 1,
    slots_per_word:,
    false_positive_rate: actual_false_positive_rate(fingerprint_bits),
    item_count: 0,
    seed: initial_seed,
    hash_function_pair:,
  ))
}

/// Inserts an item into the `CuckooFilter`.
///
/// Fails with `FilterFull` once the filter is too densely packed to make room,
/// which becomes likely past a load factor of ~95%. The filter passed in is
/// left untouched in that case, so it keeps reporting every item it already
/// holds.
///
/// Inserting the same item twice stores it twice. That is what lets `delete`
/// undo exactly one insertion, but it also means duplicates consume slots: an
/// item inserted `2 * bucket_size` times fills both of its candidate buckets,
/// and every further copy fails however empty the rest of the filter is.
///
/// * `filter`: The `CuckooFilter` to insert into.
/// * `item`: The item to insert.
pub fn insert(
  in filter: CuckooFilter(a),
  insert item: a,
) -> Result(CuckooFilter(a), CuckooFilterError) {
  let fingerprint = fingerprint(filter, item)
  let bucket = primary_bucket(filter, item)
  let partner = alternate_bucket(in: filter, of: bucket, for: fingerprint)

  let free =
    find_in_bucket(in: filter, bucket:, fingerprint: empty_slot)
    |> result.lazy_or(fn() {
      find_in_bucket(in: filter, bucket: partner, fingerprint: empty_slot)
    })

  case free {
    Ok(slot) -> Ok(store(in: filter, at: slot, fingerprint:))
    Error(Nil) -> {
      // Both candidates are full: evict a fingerprint from one of them at
      // random and let it look for room in *its* alternate bucket.
      let seed = scramble(filter.seed)
      let evict_from = case int.bitwise_and(seed, 1) {
        0 -> bucket
        _ -> partner
      }

      evict(
        in: CuckooFilter(..filter, seed:),
        place: fingerprint,
        from: evict_from,
        kicks_left: max_kicks,
      )
    }
  }
}

/// Bulk inserts multiple items into the `CuckooFilter`.
///
/// All or nothing: if any item cannot be housed the whole call fails with
/// `FilterFull` and the filter passed in is returned to you unchanged.
///
/// * `filter`: The `CuckooFilter` to insert into.
/// * `items`: The list of items to insert.
pub fn insert_many(
  in filter: CuckooFilter(a),
  insert items: List(a),
) -> Result(CuckooFilter(a), CuckooFilterError) {
  use filter, item <- list.try_fold(items, filter)
  insert(in: filter, insert: item)
}

/// Checks if the `CuckooFilter` might contain the given `item`.
///
/// * `filter`: The `CuckooFilter` to check
/// * `item`: The item to check for
pub fn might_contain(in filter: CuckooFilter(a), search item: a) -> Bool {
  find_item_slot(filter, item) |> result.is_ok
}

/// Removes one previously inserted copy of `item` from the `CuckooFilter`,
/// returning the updated filter. Fails with `ItemNotPresent` when `item`'s
/// fingerprint is absent from both its candidate buckets, i.e. the filter does
/// not hold it.
///
/// Only ever delete items you have actually inserted! A filter reports
/// membership by fingerprint, so deleting a never-inserted item that happens to
/// be a false positive drops another item's fingerprint, and that item is then
/// reported as missing – the false negatives a Cuckoo filter otherwise rules
/// out.
///
/// `Ok` therefore means "a matching fingerprint was cleared", not "you deleted
/// your own item": the false-positive case above still returns `Ok` while
/// corrupting the filter, because it is indistinguishable from a real hit. The
/// `ItemNotPresent` error only catches deletes of an item whose fingerprint is
/// wholly absent – a double delete or a wrong key. It is a misuse check, not a
/// guarantee.
///
/// * `filter`: The `CuckooFilter` to remove from
/// * `item`: The item to remove
pub fn delete(
  from filter: CuckooFilter(a),
  delete item: a,
) -> Result(CuckooFilter(a), CuckooFilterError) {
  case find_item_slot(filter, item) {
    Ok(slot) ->
      Ok(
        CuckooFilter(
          ..filter,
          array: write_slot(in: filter, at: slot, fingerprint: empty_slot),
          item_count: filter.item_count - 1,
        ),
      )
    Error(Nil) -> Error(ItemNotPresent)
  }
}

/// Returns the number of items the `CuckooFilter` has room for.
/// Insertions typically start to fail somewhat before this is reached.
///
/// * `filter`: The `CuckooFilter` from which to get the capacity
pub fn capacity(of filter: CuckooFilter(a)) -> Int {
  filter.bucket_count * bucket_size
}

/// Returns the number of items currently held by the `CuckooFilter`.
/// Unlike a Bloom filter's estimate this is exact, though an item inserted
/// twice counts twice.
///
/// * `filter`: The `CuckooFilter` from which to get the item count
pub fn item_count(in filter: CuckooFilter(a)) -> Int {
  filter.item_count
}

/// Returns the size of the `CuckooFilter`'s fingerprint storage in bits.
///
/// * `filter`: The `CuckooFilter` from which to get the size
pub fn bit_size(of filter: CuckooFilter(a)) -> Int {
  capacity(of: filter) * filter.fingerprint_bits
}

/// Returns the number of bits each of the `CuckooFilter`'s fingerprints uses.
///
/// * `filter`: The `CuckooFilter` from which to get the fingerprint width
pub fn fingerprint_bits(of filter: CuckooFilter(a)) -> Int {
  filter.fingerprint_bits
}

/// Returns the `CuckooFilter`'s false positive rate.
/// This is an upper bound, reached as the filter fills up; a sparsely
/// populated filter does better.
///
/// * `filter`: The `CuckooFilter` from which to get the error rate
pub fn false_positive_rate(of filter: CuckooFilter(a)) -> Float {
  filter.false_positive_rate
}

/// Returns an empty `CuckooFilter` with the same characteristics as the input filter.
///
/// * `filter`: The `CuckooFilter` to reset
pub fn reset(filter filter: CuckooFilter(a)) -> CuckooFilter(a) {
  CuckooFilter(
    ..filter,
    array: iv.repeat(0, word_count(filter.bucket_count, filter.slots_per_word)),
    item_count: 0,
    seed: initial_seed,
  )
}

/// The number of words needed to hold every slot of every bucket.
///
/// Ceiling division, so no trailing word is wasted when the slots divide
/// evenly. Assumes at least one slot, which `new` guarantees.
fn word_count(bucket_count: Int, slots_per_word: Int) -> Int {
  { bucket_count * bucket_size - 1 } / slots_per_word + 1
}

/// Calculates the number of bits a fingerprint needs to meet a target error
/// rate. Used in filter construction.
///
/// A lookup compares an item's fingerprint against the `2 * bucket_size`
/// fingerprints of its two candidate buckets, so `f` bits collide with
/// probability `2 * bucket_size / 2^f`.
///
/// * `target_err_rate`: The Cuckoo filter's acceptable false positive rate
fn optimal_fingerprint_bits(target_err_rate: Float) -> Int {
  // nolint: assert_ok_pattern -- log of 2.0 is defined (2.0 > 0.0)
  let assert Ok(ln_2) = float.logarithm(2.0)
  // nolint: assert_ok_pattern -- target_err_rate is validated to (0.0, 1.0)
  let assert Ok(ln_collisions) =
    float.logarithm(int.to_float(2 * bucket_size) /. target_err_rate)

  ln_collisions /. ln_2
  |> float.ceiling
  |> float.round
  |> int.clamp(min: 1, max: word_size)
}

/// Calculates the number of buckets needed to hold `capacity` items.
/// Used in filter construction.
///
/// * `capacity`: The number of elements that the filter shall be able to hold
fn optimal_bucket_count(capacity: Int) -> Int {
  let buckets_needed =
    int.to_float(capacity) /. { int.to_float(bucket_size) *. max_load_factor }
    |> float.ceiling
    |> float.round

  next_power_of_two(from: 2, to_fit: buckets_needed)
}

/// Doubles `candidate` until it reaches `target`.
/// Used to keep the bucket count a power of two.
fn next_power_of_two(from candidate: Int, to_fit target: Int) -> Int {
  use <- bool.guard(candidate >= target, candidate)
  next_power_of_two(from: candidate * 2, to_fit: target)
}

/// Calculates the false positive rate of a `CuckooFilter`.
/// Used in filter construction.
///
/// * `fingerprint_bits`: The number of bits each stored fingerprint occupies
fn actual_false_positive_rate(fingerprint_bits: Int) -> Float {
  int.to_float(2 * bucket_size)
  /. int.to_float(int.bitwise_shift_left(1, fingerprint_bits))
}

/// Returns the fingerprint the `CuckooFilter` stores for the given `item`.
///
/// * `filter`: The `CuckooFilter` whose hash functions to use
/// * `item`: The item to fingerprint
fn fingerprint(filter: CuckooFilter(a), item: a) -> Int {
  let HashFunctionPair(hash_fn_2:, ..) = filter.hash_function_pair

  // Hash functions may return negative ints; fold to a non-negative value.
  // `absolute_value` is parity-transparent, so it doesn't skew the modulo.
  // Zero marks an empty slot, so map the digest onto 1..fingerprint_mask.
  { int.absolute_value(hash_fn_2(item)) % filter.fingerprint_mask } + 1
}

/// Returns the first of the two buckets the given `item` may live in.
///
/// * `filter`: The `CuckooFilter` whose hash functions to use
/// * `item`: The item to locate
fn primary_bucket(filter: CuckooFilter(a), item: a) -> Int {
  let HashFunctionPair(hash_fn_1:, ..) = filter.hash_function_pair

  int.absolute_value(hash_fn_1(item))
  |> int.bitwise_and(filter.bucket_count - 1)
}

/// Returns the other bucket a fingerprint stored in `bucket` may live in.
///
/// XOR-ing is its own inverse, so this maps each bucket of a pair onto the
/// other. That is what lets an evicted fingerprint find its second home
/// without knowing the item it came from.
///
/// * `filter`: The `CuckooFilter` the bucket belongs to
/// * `bucket`: The bucket to find the partner of
/// * `fingerprint`: The fingerprint being placed
fn alternate_bucket(
  in filter: CuckooFilter(a),
  of bucket: Int,
  for fingerprint: Int,
) -> Int {
  scramble(fingerprint)
  |> int.bitwise_and(filter.bucket_count - 1)
  |> int.bitwise_exclusive_or(bucket)
}

/// Makes room for `fingerprint` in `bucket` by relocating one of the
/// fingerprints already there, repeating until a free slot turns up.
///
/// Each round leaves the filter holding the same fingerprints, one of them
/// homeless, so abandoning a chain that runs out of kicks loses nothing – the
/// caller keeps the filter as it was before the insertion began.
///
/// * `filter`: The `CuckooFilter` to make room in
/// * `fingerprint`: The fingerprint looking for a slot
/// * `bucket`: The bucket to evict from
/// * `kicks_left`: How many more relocations to attempt
fn evict(
  in filter: CuckooFilter(a),
  place fingerprint: Int,
  from bucket: Int,
  kicks_left kicks_left: Int,
) -> Result(CuckooFilter(a), CuckooFilterError) {
  use <- bool.guard(kicks_left == 0, Error(FilterFull))

  let seed = scramble(filter.seed)
  let slot = bucket * bucket_size + int.bitwise_and(seed, bucket_size - 1)
  let evicted = read_slot(filter, slot)
  let filter =
    CuckooFilter(
      ..filter,
      array: write_slot(in: filter, at: slot, fingerprint:),
      seed:,
    )

  let bucket = alternate_bucket(in: filter, of: bucket, for: evicted)
  case find_in_bucket(in: filter, bucket:, fingerprint: empty_slot) {
    Ok(free) -> Ok(store(in: filter, at: free, fingerprint: evicted))
    Error(Nil) ->
      evict(
        in: filter,
        place: evicted,
        from: bucket,
        kicks_left: kicks_left - 1,
      )
  }
}

/// Writes a fingerprint into a free slot, counting it as one more held item.
///
/// * `filter`: The `CuckooFilter` to store into
/// * `slot`: The slot to store in
/// * `fingerprint`: The fingerprint to store
fn store(
  in filter: CuckooFilter(a),
  at slot: Int,
  fingerprint fingerprint: Int,
) -> CuckooFilter(a) {
  CuckooFilter(
    ..filter,
    array: write_slot(in: filter, at: slot, fingerprint:),
    item_count: filter.item_count + 1,
  )
}

/// Returns the slot holding a fingerprint of `item`, if either of the two
/// buckets the item may live in has one.
///
/// * `filter`: The `CuckooFilter` to search
/// * `item`: The item to look for
fn find_item_slot(filter: CuckooFilter(a), item: a) -> Result(Int, Nil) {
  let fingerprint = fingerprint(filter, item)
  let bucket = primary_bucket(filter, item)

  find_in_bucket(in: filter, bucket:, fingerprint:)
  |> result.lazy_or(fn() {
    let partner = alternate_bucket(in: filter, of: bucket, for: fingerprint)
    find_in_bucket(in: filter, bucket: partner, fingerprint:)
  })
}

/// Returns the first slot of `bucket` holding `fingerprint`, if any.
/// Searching for `empty_slot` finds the first unoccupied slot.
///
/// * `filter`: The `CuckooFilter` to search
/// * `bucket`: The bucket to search in
/// * `fingerprint`: The fingerprint to search for
fn find_in_bucket(
  in filter: CuckooFilter(a),
  bucket bucket: Int,
  fingerprint fingerprint: Int,
) -> Result(Int, Nil) {
  find_in_bucket_loop(
    in: filter,
    at: bucket * bucket_size,
    slots_left: bucket_size,
    fingerprint:,
  )
}

fn find_in_bucket_loop(
  in filter: CuckooFilter(a),
  at slot: Int,
  slots_left slots_left: Int,
  fingerprint fingerprint: Int,
) -> Result(Int, Nil) {
  use <- bool.guard(slots_left == 0, Error(Nil))

  case read_slot(filter, slot) == fingerprint {
    True -> Ok(slot)
    False ->
      find_in_bucket_loop(
        in: filter,
        at: slot + 1,
        slots_left: slots_left - 1,
        fingerprint:,
      )
  }
}

/// Returns the fingerprint stored in the given slot, or `empty_slot`.
///
/// * `filter`: The `CuckooFilter` to read from
/// * `slot`: The slot to read
fn read_slot(filter: CuckooFilter(a), slot: Int) -> Int {
  let word = iv.get_or_default(filter.array, slot / filter.slots_per_word, 0)

  int.bitwise_shift_right(word, slot_shift(filter, slot))
  |> int.bitwise_and(filter.fingerprint_mask)
}

/// Returns the array with `fingerprint` written into the given slot,
/// replacing whatever was there.
///
/// * `filter`: The `CuckooFilter` to write to
/// * `slot`: The slot to write
/// * `fingerprint`: The fingerprint to write
fn write_slot(
  in filter: CuckooFilter(a),
  at slot: Int,
  fingerprint fingerprint: Int,
) -> Array(Int) {
  let shift = slot_shift(filter, slot)

  use word <- iv.try_update(filter.array, slot / filter.slots_per_word)
  let current =
    int.bitwise_shift_right(word, shift)
    |> int.bitwise_and(filter.fingerprint_mask)

  // XOR clears precisely the bits the slot currently holds, leaving it blank
  // for the new fingerprint. Both words stay non-negative, unlike the AND-NOT
  // this replaces, whose mask would be a negative bignum on the JS target.
  int.bitwise_exclusive_or(word, int.bitwise_shift_left(current, shift))
  |> int.bitwise_or(int.bitwise_shift_left(fingerprint, shift))
}

/// Returns how far into its word a slot's fingerprint starts.
///
/// Slots never straddle a word boundary: the `word_size % fingerprint_bits`
/// leftover bits of each word are simply left unused, which keeps reads and
/// writes to a single word and every intermediate below 2^52.
///
/// * `filter`: The `CuckooFilter` the slot belongs to
/// * `slot`: The slot to locate
fn slot_shift(filter: CuckooFilter(a), slot: Int) -> Int {
  { slot % filter.slots_per_word } * filter.fingerprint_bits
}

/// One round of xorshift32.
///
/// Drives the choice of which fingerprint to evict, and doubles as the mixer
/// spreading a fingerprint over the buckets. Shifts and XORs only, so every
/// intermediate stays below 2^45 and is exact on the JavaScript target too.
///
/// Being a bijection over 32 bits, it maps only 0 to 0 – so a fingerprint,
/// which is never 0, never mixes down to nothing.
///
/// * `value`: The state to advance, or the value to mix
fn scramble(value: Int) -> Int {
  let state = int.bitwise_and(value, u32_mask)
  let state =
    int.bitwise_exclusive_or(state, int.bitwise_shift_left(state, 13))
    |> int.bitwise_and(u32_mask)
  let state =
    int.bitwise_exclusive_or(state, int.bitwise_shift_right(state, 17))

  int.bitwise_exclusive_or(state, int.bitwise_shift_left(state, 5))
  |> int.bitwise_and(u32_mask)
}
