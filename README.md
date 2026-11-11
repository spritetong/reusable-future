# reusable-future

[![Crates.io](https://img.shields.io/crates/v/reusable-future.svg)](https://crates.io/crates/reusable-future)
[![Documentation](https://docs.rs/reusable-future/badge.svg)](https://docs.rs/reusable-future)
[![License](https://img.shields.io/crates/l/reusable-future.svg)](LICENSE-MIT)

A high-performance, resettable, and reusable future container for Rust.

`reusable-future` decouples memory allocation from concrete future types. It provides a **type-agnostic** memory container (`ReusableFuture`) that can borrow out **type-aware** future references (`ReusableFutureRef<'a, T>`).

This enables **zero-allocation reuse** across repetitive async loops, **cross-type future switching without heap reallocations**, and **cancellation-safe execution** inside `tokio::select!` branches.

---

## ⚡ Key Highlights & Differences from Tokio's `ReusableBoxFuture`

| Feature | `reusable-future` | `tokio_util::sync::ReusableBoxFuture` |
| :--- | :--- | :--- |
| **Cross-Type Switching** | **Yes (switch between `T1`, `T2`, `T3` with 0 re-allocations)** | ❌ No (strictly locked to a single type `T`) |
| **Idle / Pending State** | **Yes (`is_empty()`, default is ZST `Pending`)** | ❌ No (must always contain an active future) |
| **In-place Reset** | **Yes (`reset()`)** | ❌ No |
| **Cancellation Safety in `select!`** | **Yes (`res = &mut slot =>`, auto-resets, no guard needed)** | ⚠️ Awkward / Error-prone |
| **Fused & Unfused Modes** | **Yes (`.fused()` default, `.unfused()` for multi-step)** | ❌ No |
| **Two Usage Models** | **Flexible: hold `ReusableFuture` (multi-type) OR `ReusableFutureRef` (single-type)** | ❌ Only single typed struct |
| **Single-Threaded Optimization** | **Yes (`ReusableRcFuture`, zero atomics)** | ❌ No (`Send` only) |
| **Arc Optimization** | **Optional `triomphe::Arc` (1 word pointer)** | Standard `alloc::sync::Arc` (2 words) |
| **Runtime Agnostic** | **Yes (0 runtime dependencies)** | Tied to Tokio |

---

## 💡 Two Usage Models

### Model 1: The Canonical Multi-Type Pattern (Hold Owner, Borrow Typed Refs)

The fundamental design of `reusable-future` is that the owner `ReusableFuture` is **completely type-agnostic** (it has no generic type parameter `T`).

A service, worker, or connection pool holds a single `ReusableFuture`, and borrows typed `ReusableFutureRef<'a, T>` handles on demand. Because the underlying memory block is untyped, you can **seamlessly switch between completely different output types and futures with zero heap reallocations**:

```rust
use reusable_future::ReusableFuture;

#[tokio::main]
async fn main() {
    // 1. Hold a single, type-agnostic memory block
    let pool = ReusableFuture::new();

    // 2. Borrow a future producing an integer
    let mut int_ref = pool.clone().borrow(|| async { 42u32 });
    assert_eq!((&mut int_ref).await, 42);
    drop(int_ref); // Releases the borrow back to the pool

    // 3. ZERO-ALLOCATION CROSS-TYPE SWITCHING:
    // Reuse the EXACT SAME memory buffer for a completely different output type (String)!
    let mut str_ref = pool.clone().borrow(|| async { String::from("hello world") });
    assert_eq!((&mut str_ref).await, "hello world");
    drop(str_ref);

    // 4. Reuse again for yet another type (Vec<u8>)
    let mut vec_ref = pool.borrow(|| async { vec![1u8, 2, 3] });
    assert_eq!((&mut vec_ref).await, vec![1, 2, 3]);
}
```

---

### Model 2: The Simplified Single-Type Pattern (Hold Typed Ref Directly)

If a loop or service only ever handles futures of a **single return type** `T` (e.g. `Result<Stream, Error>`), you do not need to hold `ReusableFuture` and repeatedly call `borrow()`.

You can directly hold a `ReusableFutureRef<'a, T>` initialized via `default()`:

```rust
use reusable_future::ReusableFutureRef;

#[tokio::main]
async fn main() {
    // Directly hold a typed slot initialized to idle (ZST Pending, 0 bytes allocated)
    let mut slot: ReusableFutureRef<i32> = ReusableFutureRef::default();
    assert!(slot.is_empty());

    // Set and await the first future
    slot.set(|| async { 10 });
    assert!(!slot.is_empty());
    assert_eq!((&mut slot).await, 10);

    // Completing a future automatically resets it back to idle (fused by default)
    assert!(slot.is_empty());

    // Reuse the existing allocated memory for another future of the same type (0 heap allocations)
    slot.set(|| async { 20 });
    assert_eq!((&mut slot).await, 20);
}
```

---

## 🛡️ Cancellation-Safe `tokio::select!` Pattern

In high-concurrency network services, running an asynchronous operation (such as a handshake or connect) directly inside `tokio::select!` leads to premature aborts and resource leaks when other branches fire first.

By keeping `ReusableFutureRef` outside the loop, the in-flight operation **persists across iterations**. Because it is fused by default, **no `if guard` or manual `reset()` is needed**:

```rust
use reusable_future::ReusableFutureRef;
use std::time::Duration;

async fn handshake_with_server() -> &'static str {
    tokio::time::sleep(Duration::from_millis(50)).await;
    "connected"
}

#[tokio::main]
async fn main() {
    let mut conn_slot: ReusableFutureRef<&'static str> = ReusableFutureRef::default();

    loop {
        // Start the operation only if currently idle
        if conn_slot.is_empty() {
            conn_slot.set(handshake_with_server);
        }

        tokio::select! {
            biased;
            // Clean syntax: auto-resets on completion, empty slot returns Poll::Pending
            res = &mut conn_slot => {
                println!("Result: {}", res);
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
                // Timer fired first!
                // conn_slot is NOT dropped, handshake continues uninterrupted!
            }
        }
    }
}
```

---

## 🔄 Fused vs Unfused Modes

* **Fused mode (Default / `slot.fused()`)**:
  When polled to completion (`Poll::Ready`), the slot automatically resets to the idle/empty state, dropping the inner future and reclaiming memory for subsequent `set()` calls. When idle, polling yields `Poll::Pending` without panicking.
* **Unfused mode (`slot.unfused()`)**:
  When polled via `slot.unfused()`, returning `Poll::Ready` will **NOT** reset the slot. The future remains active in the container, allowing it to be polled repeatedly (useful for custom multi-step state machines, streaming, or custom reset timing) until manually cleared via `slot.reset()`.

---

## 🔒 API Contract Pattern: Preventing Misuse in `select!`

To prevent API consumers from mistakenly creating temporary futures directly inside `select!` branches (which causes cancellation bugs), accept a mutable reference to `ReusableFutureRef` and return `()`:

```rust
use reusable_future::ReusableFutureRef;

pub struct Connector;

impl Connector {
    /// Connects to a target server.
    ///
    /// By returning `()` instead of a `Future`, consumers cannot mistakenly
    /// call this directly inside a `select!` branch, enforcing proper slot persistence.
    pub fn connect<'a>(
        &'a self,
        target: &'a str,
        slot: &mut ReusableFutureRef<'a, Result<(), std::io::Error>>,
    ) {
        if slot.is_empty() {
            slot.set(|| async move {
                // Multi-stage connection and handshake logic...
                Ok(())
            });
        }
    }
}
```

---

## 📦 Memory Safety & Invariants

- **Dynamic Layout Growth**: If a subsequent future requires a larger size or stricter alignment than the existing allocation, `reallocate` automatically expands the buffer safely.
- **In-place Overwrite**: When the size and alignment of a new future fit within the existing allocation, memory is reused directly with zero heap reallocations.
- **Drop Invariants**: Setting a new future or calling `reset()` automatically invokes `ptr::drop_in_place` on the old future before overwriting memory.
- **Single Borrow Enforcement**: Concurrent or recursive calls to `borrow` are prevented by runtime tracking flags (`AtomicBool`), panicking safely instead of allowing undefined behavior.
- **Zero Runtime Dependencies**: The core crate depends only on `futures-util` (with `alloc`). It does not pull in `tokio` or any heavyweight framework.

---

## 📄 License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.
