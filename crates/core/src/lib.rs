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
static HEAP_CURSOR: Cell<usize> = Cell(UnsafeCell::new(8));

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
    set_heap_cursor(8);
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
    for slot in 0..capacity {
        *table.add(slot) = -1;
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

fn last_group_count() -> usize {
    unsafe { LAST_GROUP_COUNT.0.get().read() }
}

fn heap_cursor() -> usize {
    unsafe { HEAP_CURSOR.0.get().read() }
}

fn set_heap_cursor(value: usize) {
    unsafe { HEAP_CURSOR.0.get().write(value) }
}
