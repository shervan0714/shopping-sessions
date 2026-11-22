//! Minimal parallelism: scoped threads and an atomic work counter (replaces
//! rayon). Every stage spawns `threads()` workers; spawning a handful of
//! threads per stage costs tens of microseconds, far below stage times.

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

static THREADS: AtomicUsize = AtomicUsize::new(1);

pub fn set_threads(n: usize) {
    THREADS.store(n.max(1), Relaxed);
}

pub fn threads() -> usize {
    THREADS.load(Relaxed)
}

/// Run `f(thread_index)` on `n` threads; results in thread order.
pub fn run<R: Send>(n: usize, f: impl Fn(usize) -> R + Sync) -> Vec<R> {
    if n <= 1 {
        return vec![f(0)];
    }
    std::thread::scope(|s| {
        let f = &f;
        let hs: Vec<_> = (1..n).map(|t| s.spawn(move || f(t))).collect();
        let first = f(0);
        let mut out = Vec::with_capacity(n);
        out.push(first);
        out.extend(hs.into_iter().map(|h| h.join().expect("worker panicked")));
        out
    })
}

/// `threads()` workers; each makes its state with `init(thread_index)`, then
/// takes item indices `0..items` from a shared counter, in order, calling
/// `f(&mut state, index)`. Returns the states in thread order.
pub fn each<S: Send>(items: usize, init: impl Fn(usize) -> S + Sync, f: impl Fn(&mut S, usize) + Sync) -> Vec<S> {
    let next = AtomicUsize::new(0);
    let n = threads().min(items.max(1));
    run(n, |t| {
        let mut s = init(t);
        loop {
            let i = next.fetch_add(1, Relaxed);
            if i >= items {
                break;
            }
            f(&mut s, i);
        }
        s
    })
}
