import gauzy/cuckoo_filter.{
  type CuckooFilter, type CuckooFilterError, type HashFunctionPair,
}
import gleam/int
import murmur3a

/// Creates a hash function using murmur3a with the given seed.
///
/// Items are hashed as strings rather than as the Bloom filter tests' lists of
/// integers: `murmur3a.hash_ints` masks each element down to a single byte, so
/// a list of one integer only ever yields 256 distinct digests. A Bloom filter
/// tolerates that, but a Cuckoo filter has nowhere to put the 257th item.
fn create_hash_function(seed: Int) -> fn(String) -> Int {
  fn(key) {
    key
    |> murmur3a.hash_string(seed)
    |> murmur3a.int_digest
  }
}

/// Creates a test fixture for a hash function pair.
/// Uses two hash functions with different seeds (0 and 1) for testing purposes.
fn hash_function_pair_fixture() -> HashFunctionPair(String) {
  let assert Ok(hash_fn_pair) =
    cuckoo_filter.new_hash_fn_pair(
      create_hash_function(0),
      create_hash_function(1),
    )
  hash_fn_pair
}

/// Creates a test cuckoo filter with specified capacity and target error rate.
/// Uses the hash function pair fixture for consistent testing.
fn create_test_filter(
  capacity: Int,
  target_err_rate: Float,
) -> CuckooFilter(String) {
  let assert Ok(filter) =
    cuckoo_filter.new(capacity, target_err_rate, hash_function_pair_fixture())
  filter
}

/// Inserts the items `"0"` to `"count - 1"` one at a time,
/// asserting that every insertion succeeds.
fn insert_items(
  in filter: CuckooFilter(String),
  with count: Int,
) -> CuckooFilter(String) {
  int.range(from: 0, to: count, with: filter, run: fn(filter, element) {
    let assert Ok(filter) = cuckoo_filter.insert(filter, int.to_string(element))
    filter
  })
}

/// Returns whether all items from `0` (inclusive) to `count` (exclusive)
/// are present in the filter.
fn all_items_present(in filter: CuckooFilter(String), with count: Int) -> Bool {
  int.range(from: 0, to: count, with: True, run: fn(acc, element) {
    acc && cuckoo_filter.might_contain(filter, int.to_string(element))
  })
}

/// Returns whether no items from `0` (inclusive) to `count` (exclusive)
/// are present in the filter.
fn no_items_present(in filter: CuckooFilter(String), with count: Int) -> Bool {
  int.range(from: 0, to: count, with: True, run: fn(acc, element) {
    acc && !cuckoo_filter.might_contain(filter, int.to_string(element))
  })
}

/// Returns how many items from `0` (inclusive) to `count` (exclusive) the
/// filter reports, for the checks where false positives are expected.
fn count_items_present(
  in filter: CuckooFilter(String),
  with count: Int,
) -> Int {
  int.range(from: 0, to: count, with: 0, run: fn(acc, element) {
    case cuckoo_filter.might_contain(filter, int.to_string(element)) {
      True -> acc + 1
      False -> acc
    }
  })
}

/// Verifies that a populated filter contains all expected items, counts them
/// exactly, rejects an unknown item, and clears correctly on reset.
fn verify_filter(filter: CuckooFilter(String), count: Int) -> Nil {
  assert all_items_present(in: filter, with: count)

  assert cuckoo_filter.item_count(filter) == count
  assert !cuckoo_filter.might_contain(filter, "never inserted")

  let reset_filter = cuckoo_filter.reset(filter)
  assert no_items_present(in: reset_filter, with: count)
  assert cuckoo_filter.item_count(reset_filter) == 0
}

pub fn new_hash_function_pair_test() -> Result(
  HashFunctionPair(String),
  CuckooFilterError,
) {
  let hash_fn_1 = create_hash_function(0)
  let hash_fn_2 = create_hash_function(1)

  let assert Ok(_) = cuckoo_filter.new_hash_fn_pair(hash_fn_1, hash_fn_2)
  let assert Error(_) = cuckoo_filter.new_hash_fn_pair(hash_fn_1, hash_fn_1)
}

pub fn new_cuckoo_filter_test() -> Result(
  CuckooFilter(String),
  CuckooFilterError,
) {
  // 10_000 items need 2632 buckets of 4 slots at a 95% load factor, rounded up
  // to the next power of two.
  let filter = create_test_filter(10_000, 0.001)
  assert cuckoo_filter.capacity(filter) == 16_384
  assert cuckoo_filter.fingerprint_bits(filter) == 13
  assert cuckoo_filter.bit_size(filter) == 212_992
  assert cuckoo_filter.item_count(filter) == 0
  // `2 * bucket_size / 2^fingerprint_bits` is a ratio of powers of two, so
  // unlike the Bloom filter's rate it is exact on both targets.
  assert cuckoo_filter.false_positive_rate(filter) == 0.0009765625

  let small_filter = create_test_filter(1, 0.1)
  assert cuckoo_filter.capacity(small_filter) == 8
  assert cuckoo_filter.fingerprint_bits(small_filter) == 7
  assert cuckoo_filter.bit_size(small_filter) == 56
  assert cuckoo_filter.false_positive_rate(small_filter) == 0.0625

  // Fingerprints are capped at one word, so tinier rates are clamped.
  let precise_filter = create_test_filter(100, 1.0e-30)
  assert cuckoo_filter.fingerprint_bits(precise_filter) == 52

  let hash_fn_pair = hash_function_pair_fixture()
  let assert Error(_) = cuckoo_filter.new(0, 0.5, hash_fn_pair)
  let assert Error(_) = cuckoo_filter.new(100, 0.0, hash_fn_pair)
  let assert Error(_) = cuckoo_filter.new(100, 1.0, hash_fn_pair)
}

pub fn insert_works_test() -> Nil {
  let capacity = 10_000
  let filter = create_test_filter(capacity, 0.001)

  verify_filter(insert_items(in: filter, with: capacity), capacity)
}

pub fn insert_many_works_test() -> Nil {
  let capacity = 10_000
  let filter = create_test_filter(capacity, 0.001)

  let items = {
    int.range(from: 0, to: capacity, with: [], run: fn(acc, element) {
      [int.to_string(element), ..acc]
    })
  }

  let assert Ok(filter) = cuckoo_filter.insert_many(filter, items)

  verify_filter(filter, capacity)
}

pub fn a_filter_holds_its_stated_capacity_test() -> Nil {
  // 7782 items need exactly 2048 buckets, so rounding up to a power of two
  // leaves no slack: the worst case for a filter meeting its capacity.
  let filter = create_test_filter(7782, 0.001)
  assert cuckoo_filter.capacity(filter) == 8192

  let #(_, inserted) = fill_until_full(filter, 0)
  assert inserted >= 7782
}

pub fn delete_works_test() -> Nil {
  let inserted = 2000
  let deleted = 1000
  let filter = create_test_filter(10_000, 0.001)
  let filter = insert_items(in: filter, with: inserted)

  let filter =
    int.range(from: 0, to: deleted, with: filter, run: fn(filter, element) {
      let assert Ok(filter) =
        cuckoo_filter.delete(filter, int.to_string(element))
      filter
    })

  assert cuckoo_filter.item_count(filter) == inserted - deleted

  // Deleting never removes an item that was not deleted.
  assert int.range(
    from: deleted,
    to: inserted,
    with: True,
    run: fn(acc, element) {
      acc && cuckoo_filter.might_contain(filter, int.to_string(element))
    },
  )

  // The deleted items are gone, bar the odd false positive: another item's
  // fingerprint may happen to sit in one of their candidate buckets.
  assert count_items_present(in: filter, with: deleted) <= 5
}

pub fn delete_missing_item_errors_test() -> Nil {
  let filter = create_test_filter(100, 0.01)
  let filter = insert_items(in: filter, with: 10)

  // The item's fingerprint is absent, so the delete is rejected outright.
  assert cuckoo_filter.delete(filter, "never inserted")
    == Error(cuckoo_filter.ItemNotPresent)

  // ...and the filter is left untouched.
  assert cuckoo_filter.item_count(filter) == 10
  assert all_items_present(in: filter, with: 10)
}

pub fn delete_removes_one_copy_at_a_time_test() -> Nil {
  let filter = create_test_filter(100, 0.01)
  let assert Ok(filter) = cuckoo_filter.insert(filter, "twice")
  let assert Ok(filter) = cuckoo_filter.insert(filter, "twice")
  assert cuckoo_filter.item_count(filter) == 2

  let assert Ok(filter) = cuckoo_filter.delete(filter, "twice")
  assert cuckoo_filter.item_count(filter) == 1
  assert cuckoo_filter.might_contain(filter, "twice")

  let assert Ok(filter) = cuckoo_filter.delete(filter, "twice")
  assert cuckoo_filter.item_count(filter) == 0
  assert !cuckoo_filter.might_contain(filter, "twice")
}

pub fn duplicates_fill_both_candidate_buckets_test() -> Nil {
  let filter = create_test_filter(100, 0.01)

  // An item has two candidate buckets of four slots, so eight copies fit...
  let filter =
    int.range(from: 0, to: 8, with: filter, run: fn(filter, _) {
      let assert Ok(filter) = cuckoo_filter.insert(filter, "duplicate")
      filter
    })
  assert cuckoo_filter.item_count(filter) == 8

  // ...and the ninth has nowhere to go, however empty the rest of the filter is.
  let assert Error(_) = cuckoo_filter.insert(filter, "duplicate")
  assert cuckoo_filter.item_count(filter) == 8

  Nil
}

pub fn a_full_filter_stays_intact_test() -> Nil {
  let filter = create_test_filter(100, 0.01)
  let capacity = cuckoo_filter.capacity(filter)
  let #(filter, inserted) = fill_until_full(filter, 0)

  // A Cuckoo filter gives up shortly before every last slot is taken.
  assert inserted < capacity
  assert inserted >= capacity * 9 / 10

  // The failed insertion left the filter it was handed untouched, so nothing
  // was lost in the abandoned chain of evictions.
  assert cuckoo_filter.item_count(filter) == inserted
  assert all_items_present(in: filter, with: inserted)
}

pub fn insert_many_keeps_the_filter_on_failure_test() -> Nil {
  let filter = create_test_filter(100, 0.01)
  let capacity = cuckoo_filter.capacity(filter)

  let too_many = {
    int.range(from: 0, to: capacity + 1, with: [], run: fn(acc, element) {
      [int.to_string(element), ..acc]
    })
  }

  let assert Error(_) = cuckoo_filter.insert_many(filter, too_many)
  assert cuckoo_filter.item_count(filter) == 0

  Nil
}

/// Inserts `"0"`, `"1"`, … until an insertion fails, returning the last filter
/// to accept an item and the number of items it holds.
fn fill_until_full(
  filter: CuckooFilter(String),
  next: Int,
) -> #(CuckooFilter(String), Int) {
  case cuckoo_filter.insert(filter, int.to_string(next)) {
    Ok(filter) -> fill_until_full(filter, next + 1)
    // nolint: thrown_away_error -- running out of room is the expected outcome
    Error(_) -> #(filter, next)
  }
}
