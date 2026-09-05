//! Dedicated process: allocation failure must never terminate the server.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use hubuum_templates::MAX_WORKER_HEAP_BYTES;

struct BudgetAllocator;
static PEAK: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

fn reserve(size: usize) -> bool {
    ALLOCATED
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current
                .checked_add(size)
                .filter(|next| *next <= MAX_WORKER_HEAP_BYTES)
        })
        .map(|previous| {
            PEAK.fetch_max(previous + size, Ordering::Relaxed);
        })
        .is_ok()
}

// SAFETY: allocation and deallocation always use System with the original
// layout. Accounting reserves capacity before System can allocate, and releases
// it only after deallocation; realloc reserves only positive growth. Failed
// allocation rolls back its reservation and follows GlobalAlloc's null contract.
unsafe impl GlobalAlloc for BudgetAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if !reserve(layout.size()) {
            // Deliberate budget exhaustion exits without an abort/core dump.
            std::process::exit(70);
        }
        // SAFETY: caller supplies GlobalAlloc's valid layout.
        let pointer = unsafe { System.alloc(layout) };
        if pointer.is_null() {
            ALLOCATED.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: this allocator delegates every successful allocation to System.
        unsafe { System.dealloc(pointer, layout) };
        ALLOCATED.fetch_sub(layout.size(), Ordering::Relaxed);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let growth = new_size.saturating_sub(layout.size());
        if !reserve(growth) {
            std::process::exit(70);
        }
        // SAFETY: pointer/layout are the original System allocation and new_size
        // satisfies the caller's GlobalAlloc realloc contract.
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if resized.is_null() {
            ALLOCATED.fetch_sub(growth, Ordering::Relaxed);
        } else if new_size < layout.size() {
            ALLOCATED.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
        }
        resized
    }
}

#[global_allocator]
static ALLOCATOR: BudgetAllocator = BudgetAllocator;

fn main() {
    if std::env::args()
        .nth(1)
        .is_some_and(|value| value == "--version" || value == "--help")
    {
        println!(
            "hubuum-template-worker {}: bounded template execution protocol",
            env!("CARGO_PKG_VERSION")
        );
        return;
    }
    if hubuum_templates::serve_template_worker(|| PEAK.load(Ordering::Relaxed)).is_err() {
        std::process::exit(1);
    }
}
