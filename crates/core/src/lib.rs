//! WASM kernels for bun_panda hot paths.
//!
//! Flat C-style ABI over linear memory. No bindgen, no wasm-bindgen:
//! callers pass raw pointers obtained from `bp_alloc` and released with
//! `bp_free`. Missing numeric values are encoded as IEEE754 NaN in f64
//! arrays, matching the TS side where `NaN` never occurs in real data
//! (`utils.isNumber` only accepts finite numbers).
//!
//! Layout conventions:
//! - `ids` arrays are `i32` group indices in `[0, n_groups)`; `-1` marks
//!   rows excluded from aggregation (missing keys when `dropna`).
//! - `offsets` arrays are `i32` byte offsets with `len == n + 1`.
//! - Output buffers are caller-allocated, exactly `n_groups` wide, and
//!   MUST be zeroed by the caller (the bump arena returns dirty pages).

#![no_std]
#![allow(clippy::missing_safety_doc)]
#![allow(unsafe_op_in_unsafe_fn)]

use core::cell::UnsafeCell;
use core::panic::PanicInfo;
use core::slice;

#[panic_handler]
fn panic(_info: &PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

/// Aggregation codes shared with src/wasm/kernel.ts.
pub const AGG_SUM: i32 = 0;
pub const AGG_MEAN: i32 = 1;
pub const AGG_MIN: i32 = 2;
pub const AGG_MAX: i32 = 3;
pub const AGG_COUNT: i32 = 4;

struct Cell<T>(UnsafeCell<T>);
unsafe impl<T> Sync for Cell<T> {}

static LAST_GROUP_COUNT: Cell<usize> = Cell(UnsafeCell::new(0));
// `wasm-ld` defines this at the first byte after the module's static data.
// Starting the arena below it corrupts globals once an allocation crosses
// the data segment (large sorts exposed this at roughly 125k f64 values).
unsafe extern "C" {
    static __heap_base: u8;
}

static HEAP_CURSOR: Cell<usize> = Cell(UnsafeCell::new(0));

/// Bump allocator over fresh linear-memory pages. All allocations live
/// until [`bp_free_all`]; individual frees only rewind the cursor when
/// the block happens to be the most recent one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bp_alloc(len: usize) -> *mut u8 {
    if len == 0 {
        return heap_cursor() as *mut u8;
    }
    let base = (heap_cursor() + 15) & !15;
    let end = base + len;
    let page = 65_536usize;
    let needed_pages = (end + page - 1) / page;
    let current_pages = core::arch::wasm32::memory_size(0);
    if needed_pages > current_pages {
        let delta = needed_pages - current_pages;
        if core::arch::wasm32::memory_grow(0, delta) == usize::MAX {
            return core::ptr::null_mut();
        }
    }
    set_heap_cursor(end);
    base as *mut u8
}

/// Releases one bump allocation; see [`bp_alloc`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bp_free(ptr: *mut u8, len: usize) {
    if ptr.is_null() || len == 0 {
        return;
    }
    let end = ptr as usize + len;
    if end == heap_cursor() {
        set_heap_cursor(ptr as usize);
    }
}

/// Resets the arena. Call between top-level operations, never while a
/// previously returned buffer is still readable.
#[unsafe(no_mangle)]
pub extern "C" fn bp_free_all() {
    set_heap_cursor(heap_start());
}

/// Number of distinct groups produced by the most recent
/// [`bp_group_ids`] call.
#[unsafe(no_mangle)]
pub extern "C" fn bp_last_group_count() -> i32 {
    unsafe { LAST_GROUP_COUNT.0.get().read() as i32 }
}

/// Assigns a dense group id per row from packed key bytes.
///
/// `keys` holds every row's key fragment back-to-back; `offsets`
/// delimits each row's slice (`n + 1` entries). An empty fragment marks
/// a missing key: such rows get id `-1` and are excluded from every
/// aggregate.
///
/// Returns a pointer to `n` i32 ids inside the arena.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bp_group_ids(keys: *const u8, offsets: *const i32, n: usize) -> *mut i32 {
    let total = *(offsets.add(n)) as usize;
    let keys_slice = slice::from_raw_parts(keys, total);
    let offs = slice::from_raw_parts(offsets, n + 1);

    let capacity = (n * 2 + 16).next_power_of_two();
    let mask = capacity - 1;
    let table = bp_alloc(capacity * 4) as *mut i32; // slot -> group id, -1 empty
    let hashes = bp_alloc(capacity * 8) as *mut u64; // slot -> full hash
    let first_row = bp_alloc((n.max(1)) * 8) as *mut usize; // group id -> representative row
    let ids = bp_alloc(n.max(1) * 4) as *mut i32;
    if table.is_null() || hashes.is_null() || first_row.is_null() || ids.is_null() {
        return core::ptr::null_mut();
    }
    // Generation counter: instead of memsetting `capacity` slots to -1 on
    // every call, tag each occupied slot with the current generation. A
    // slot whose tag differs is logically empty. The table is allocated
    // fresh from the bump arena each call, so its bytes are dirty — but
    // we only need `hashes` zeroed once per page, which is far cheaper
    // than a full pass when groups are few. For simplicity and safety
    // (dirty bytes could alias a valid group tag), keep the clearing pass
    // but write it as an unrolled word-at-a-time loop.
    {
        let words = table as *mut i64;
        let byte_len = capacity * 4;
        let mut w = 0;
        while w + 8 <= byte_len {
            *(words.add(w / 8)) = -1i64; // two i32 -1s per word
            w += 8;
        }
        if w < byte_len {
            *(words.add(w / 8)) = -1i64;
        }
    }

    let mut group_count: usize = 0;

    for row in 0..n {
        let start = offs[row] as usize;
        let end = offs[row + 1] as usize;

        // Missing key: empty fragment maps to id -1.
        if start == end {
            *ids.add(row) = -1;
            continue;
        }

        let bytes = &keys_slice[start..end];
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in bytes {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x1000_0000_01b3);
        }

        let mut slot = (hash as usize) & mask;
        loop {
            let occupant = *table.add(slot);
            if occupant < 0 {
                // New group: remember hash, representative row, assign id.
                *table.add(slot) = group_count as i32;
                *hashes.add(slot) = hash;
                *first_row.add(group_count) = row;
                *ids.add(row) = group_count as i32;
                group_count += 1;
                break;
            }
            if *hashes.add(slot) == hash {
                // Full byte-comparison against the group's representative
                // row keeps results exact even across hash collisions.
                let rep = *first_row.add(occupant as usize);
                let rep_start = offs[rep] as usize;
                let rep_end = offs[rep + 1] as usize;
                if keys_slice[rep_start..rep_end] == *bytes {
                    *ids.add(row) = occupant;
                    break;
                }
            }
            slot = (slot + 1) & mask;
        }
    }

    unsafe {
        LAST_GROUP_COUNT.0.get().write(group_count);
    }
    ids
}

/// Aggregates an f64 column per group. NaN encodes a missing value and is
/// skipped for every code. `out` receives one f64 per group; groups with
/// no contributing values are NaN for sum/mean/min/max and 0 for count.
/// Caller MUST zero both `out` and `counts` beforehand.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bp_agg_f64(
    values: *const f64,
    ids: *const i32,
    n: usize,
    code: i32,
    out: *mut f64,
    counts: *mut i32,
    n_groups: usize,
) {
    let vals = slice::from_raw_parts(values, n);
    let group_ids = slice::from_raw_parts(ids, n);

    match code {
        AGG_SUM | AGG_MEAN => {
            for i in 0..n {
                let v = vals[i];
                if v.is_nan() {
                    continue;
                }
                let g = group_ids[i];
                if g < 0 {
                    continue;
                }
                let g = g as usize;
                *out.add(g) += v;
                *counts.add(g) += 1;
            }
            if code == AGG_MEAN {
                for g in 0..n_groups {
                    let c = *counts.add(g);
                    if c > 0 {
                        *out.add(g) /= c as f64;
                    } else {
                        *out.add(g) = f64::NAN;
                    }
                }
            } else {
                for g in 0..n_groups {
                    if *counts.add(g) == 0 {
                        *out.add(g) = f64::NAN;
                    }
                }
            }
        }
        AGG_MIN => aggregate_extreme(out, counts, group_ids, vals, n, false),
        AGG_MAX => aggregate_extreme(out, counts, group_ids, vals, n, true),
        AGG_COUNT => {
            for i in 0..n {
                let v = vals[i];
                if v.is_nan() {
                    continue;
                }
                let g = group_ids[i];
                if g >= 0 {
                    *counts.add(g as usize) += 1;
                }
            }
            for g in 0..n_groups {
                *out.add(g) = *counts.add(g) as f64;
            }
        }
        _ => {}
    }
}

unsafe fn aggregate_extreme(
    out: *mut f64,
    counts: *mut i32,
    group_ids: &[i32],
    vals: &[f64],
    n: usize,
    is_max: bool,
) {
    for i in 0..n {
        let v = vals[i];
        if v.is_nan() {
            continue;
        }
        let g = group_ids[i];
        if g < 0 {
            continue;
        }
        let g = g as usize;
        if *counts.add(g) == 0 {
            *out.add(g) = v;
        } else if (is_max && v > *out.add(g)) || (!is_max && v < *out.add(g)) {
            *out.add(g) = v;
        }
        *counts.add(g) = 1;
    }
    for g in 0..last_group_count() {
        if *counts.add(g) == 0 {
            *out.add(g) = f64::NAN;
        }
    }
}

/// Aggregates multiple f64 columns per group in one pass over the data.
///
/// `values` points to `n_plans` consecutive column buffers of length
/// `n` (column-major); `plan_codes` has one aggregation code per plan.
/// `out` is `n_groups * n_plans` floats, plan-major; `counts` is
/// `n_groups * n_plans` i32. Caller MUST zero both. NaN = missing.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bp_agg_multi_f64(
    values: *const f64,
    ids: *const i32,
    n: usize,
    plan_codes: *const i32,
    n_plans: usize,
    out: *mut f64,
    counts: *mut i32,
    n_groups: usize,
) {
    let codes = slice::from_raw_parts(plan_codes, n_plans);
    let group_ids = slice::from_raw_parts(ids, n);
    // Plan-major output: plan p's buffer starts at out.add(p * n_groups).
    for p in 0..n_plans {
        let col = slice::from_raw_parts(values.add(p * n), n);
        let code = codes[p];
        let out_p = out.add(p * n_groups);
        let cnt_p = counts.add(p * n_groups);
        match code {
            AGG_SUM | AGG_MEAN => {
                for i in 0..n {
                    let v = col[i];
                    if v.is_nan() {
                        continue;
                    }
                    let g = group_ids[i];
                    if g < 0 {
                        continue;
                    }
                    let g = g as usize;
                    *out_p.add(g) += v;
                    *cnt_p.add(g) += 1;
                }
                if code == AGG_MEAN {
                    for g in 0..n_groups {
                        let c = *cnt_p.add(g);
                        if c > 0 {
                            *out_p.add(g) /= c as f64;
                        } else {
                            *out_p.add(g) = f64::NAN;
                        }
                    }
                } else {
                    for g in 0..n_groups {
                        if *cnt_p.add(g) == 0 {
                            *out_p.add(g) = f64::NAN;
                        }
                    }
                }
            }
            AGG_MIN => aggregate_extreme(out_p, cnt_p, group_ids, col, n, false),
            AGG_MAX => aggregate_extreme(out_p, cnt_p, group_ids, col, n, true),
            AGG_COUNT => {
                for i in 0..n {
                    let v = col[i];
                    if v.is_nan() {
                        continue;
                    }
                    let g = group_ids[i];
                    if g >= 0 {
                        *cnt_p.add(g as usize) += 1;
                    }
                }
                for g in 0..n_groups {
                    *out_p.add(g) = *cnt_p.add(g) as f64;
                }
            }
            _ => {}
        }
    }
}

/// Argsort of an f64 column (NaN sorted last), ascending or descending.
///
/// Returns a pointer to `n` u16-pair-free plain i32 indices inside the
/// arena; rows are ordered so that `values[idx[0]] <= values[idx[1]]...`
/// NaN entries go to the end regardless of direction (pandas
/// `na_position="last"` default). Stable: ties keep source order via a
/// merge sort on index/value pairs.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bp_argsort_f64(values: *const f64, n: usize, ascending: i32) -> *mut i32 {
    let vals = slice::from_raw_parts(values, n);
    let idx = bp_alloc(n.max(1) * 4) as *mut i32;
    if idx.is_null() {
        return core::ptr::null_mut();
    }

    // Partition into finite prefix and NaN suffix, remembering order.
    let mut finite = 0usize;
    let mut nan_count = 0usize;
    for i in 0..n {
        if vals[i].is_nan() {
            nan_count += 1;
        } else {
            finite += 1;
        }
    }

    // Simple stable approach: copy indices of finite values, then do a
    // bottom-up merge sort comparing by value; append NaN indices after.
    let tmp = bp_alloc(finite.max(1) * 4) as *mut i32;
    if tmp.is_null() {
        return core::ptr::null_mut();
    }

    let mut w = 0usize;
    for i in 0..n {
        if !vals[i].is_nan() {
            *idx.add(w) = i as i32;
            w += 1;
        }
    }

    // Bottom-up merge sort (stable).
    let mut width = 1usize;
    while width < finite {
        let mut lo = 0usize;
        while lo < finite {
            let mid = (lo + width).min(finite);
            let hi = (lo + 2 * width).min(finite);
            merge_run(vals, idx, tmp, lo, mid, hi, ascending != 0);
            lo += 2 * width;
        }
        width *= 2;
    }

    // Append NaN indices at the end, preserving source order.
    let mut tail = finite;
    for i in 0..n {
        if vals[i].is_nan() {
            *idx.add(tail) = i as i32;
            tail += 1;
        }
    }
    debug_assert_eq!(tail, n);

    idx
}

unsafe fn merge_run(
    vals: &[f64],
    idx: *mut i32,
    tmp: *mut i32,
    lo: usize,
    mid: usize,
    hi: usize,
    ascending: bool,
) {
    // Copy left run into tmp.
    for t in lo..mid {
        *tmp.add(t - lo) = *idx.add(t);
    }
    let mut i = lo;
    let mut l = 0usize;
    let mut r = mid;
    while l < mid - lo && r < hi {
        let lv = vals[*tmp.add(l) as usize];
        let rv = vals[*idx.add(r) as usize];
        let take_left = if ascending { lv <= rv } else { lv >= rv };
        if take_left {
            *idx.add(i) = *tmp.add(l);
            l += 1;
        } else {
            *idx.add(i) = *idx.add(r);
            r += 1;
        }
        i += 1;
    }
    while l < mid - lo {
        *idx.add(i) = *tmp.add(l);
        l += 1;
        i += 1;
    }
    while r < hi {
        *idx.add(i) = *idx.add(r);
        r += 1;
        i += 1;
    }
}

/// Boolean mask filter producing compacted row indices.
///
/// `mask` is `n` bytes (0 = drop, nonzero = keep). Returns indices of
/// kept rows inside the arena; count via `bp_last_group_count`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bp_filter_indices(mask: *const u8, n: usize) -> *mut i32 {
    let flags = slice::from_raw_parts(mask, n);
    let idx = bp_alloc(n.max(1) * 4) as *mut i32;
    if idx.is_null() {
        return core::ptr::null_mut();
    }
    let mut w = 0usize;
    for i in 0..n {
        if flags[i] != 0 {
            *idx.add(w) = i as i32;
            w += 1;
        }
    }
    unsafe {
        LAST_GROUP_COUNT.0.get().write(w);
    }
    idx
}

fn last_group_count() -> usize {
    unsafe { LAST_GROUP_COUNT.0.get().read() }
}

fn heap_cursor() -> usize {
    let cursor = unsafe { HEAP_CURSOR.0.get().read() };
    if cursor == 0 { heap_start() } else { cursor }
}

fn set_heap_cursor(value: usize) {
    unsafe { HEAP_CURSOR.0.get().write(value) }
}

fn heap_start() -> usize {
    core::ptr::addr_of!(__heap_base) as usize
}
