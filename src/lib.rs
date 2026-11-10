// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MIT OR Apache-2.0

//! # reusable-future
//!
//! A high-performance, resettable, and reusable future container for Rust.
//!
//! `reusable-future` decouples memory allocation from concrete future types. It provides
//! a **type-agnostic** memory container (`ReusableFuture`) that can borrow out **type-aware**
//! future references (`ReusableFutureRef<'a, T>`).
//!
//! This enables **zero-allocation reuse** across repetitive async loops, **cross-type future
//! switching without heap reallocations**, and **cancellation-safe execution** inside
//! `tokio::select!` branches.
//!
//! ---
//!
//! ## 💡 Core Design Philosophy: Two Usage Models
//!
//! ### Model 1: The Canonical Multi-Type Pattern (Hold Owner, Borrow Typed Refs)
//! The fundamental design of `reusable-future` is that the owner `ReusableFuture` is **completely
//! type-agnostic** (it has no generic type parameter `T`).
//!
//! A struct or worker holds a single `ReusableFuture`, and borrows typed `ReusableFutureRef<'a, T>`
//! handles on demand. Because the underlying memory block is untyped, you can **seamlessly switch
//! between completely different output types and futures with zero heap reallocations**:
//!
//! ```rust
//! use reusable_future::ReusableFuture;
//!
//! # #[tokio::main]
//! # async fn main() {
//! // 1. Hold a single, type-agnostic memory block
//! let pool = ReusableFuture::new();
//!
//! // 2. Borrow a future producing an integer
//! let mut int_ref = pool.clone().borrow(|| async { 42u32 });
//! assert_eq!((&mut int_ref).await, 42);
//! drop(int_ref); // Releases the borrow back to the pool
//!
//! // 3. CROSS-TYPE SWITCHING (0 Heap Allocations):
//! // Reuse the EXACT SAME memory buffer for a completely different output type (String)!
//! let mut str_ref = pool.clone().borrow(|| async { String::from("hello world") });
//! assert_eq!((&mut str_ref).await, "hello world");
//! drop(str_ref);
//!
//! // 4. Reuse again for yet another type (Vec<u8>)
//! let mut vec_ref = pool.borrow(|| async { vec![1u8, 2, 3] });
//! assert_eq!((&mut vec_ref).await, vec![1, 2, 3]);
//! # }
//! ```
//!
//! ### Model 2: The Simplified Single-Type Pattern (Hold Typed Ref Directly)
//! If a loop or service only ever handles futures of a **single return type** `T` (e.g. `Result<Stream, Error>`),
//! you do not need to hold `ReusableFuture` and repeatedly call `borrow()`.
//!
//! You can directly hold a `ReusableFutureRef<'a, T>` initialized via `default()`:
//!
//! ```rust
//! use reusable_future::ReusableFutureRef;
//!
//! # #[tokio::main]
//! # async fn main() {
//! // Directly hold a typed slot initialized to idle (ZST Pending, 0 bytes allocated)
//! let mut slot: ReusableFutureRef<i32> = ReusableFutureRef::default();
//! assert!(slot.is_empty());
//!
//! // Set and await the first future
//! slot.set(|| async { 10 });
//! assert!(!slot.is_empty());
//! assert_eq!((&mut slot).await, 10);
//!
//! // Reset back to empty without deallocating the underlying memory block
//! slot.reset();
//! assert!(slot.is_empty());
//!
//! // Reuse the existing allocated memory for another future of the same type (0 heap allocations)
//! slot.set(|| async { 20 });
//! assert_eq!((&mut slot).await, 20);
//! # }
//! ```
//!
//! ---
//!
//! ## 📊 Key Differences from Tokio's `ReusableBoxFuture`
//!
//! | Feature | `reusable-future` | `tokio_util::sync::ReusableBoxFuture` |
//! | :--- | :--- | :--- |
//! | **Cross-Type Switching** | **Yes (freely switch between `T1`, `T2`, `T3` with 0 re-allocations)** | ❌ No (strictly locked to a single type `T`) |
//! | **Idle / Pending State** | **Yes (`is_empty()`, default is ZST `Pending`)** | ❌ No (must always contain an active future) |
//! | **In-place Reset** | **Yes (`reset()`)** | ❌ No |
//! | **Cancellation Safety in `select!`** | **Yes (`if !slot.is_empty()`)** | ⚠️ Awkward / Error-prone |
//! | **Two Usage Models** | **Flexible: hold `ReusableFuture` (multi-type) OR `ReusableFutureRef` (single-type)** | ❌ Only single typed struct |
//! | **Single-Threaded Optimization** | **Yes (`ReusableRcFuture`, zero atomics)** | ❌ No (`Send` only) |
//! | **Arc Optimization** | **Optional `triomphe::Arc` (1 word pointer)** | Standard `alloc::sync::Arc` (2 words) |
//! | **Runtime Agnostic** | **Yes (0 runtime dependencies)** | Tied to Tokio |
//!
//! ---
//!
//! ## 🛡️ Cancel-Safe `tokio::select!` Loop Pattern
//!
//! In high-concurrency network services, running an asynchronous operation (such as a handshake
//! or connect) inside `tokio::select!` easily leads to premature aborts if other branches fire.
//! By keeping `ReusableFutureRef` outside the loop, the in-flight operation **persists across iterations**:
//!
//! ```rust
//! use reusable_future::ReusableFutureRef;
//! use std::time::Duration;
//!
//! async fn connect_server() -> &'static str {
//!     tokio::time::sleep(Duration::from_millis(50)).await;
//!     "connected"
//! }
//!
//! # #[tokio::main]
//! # async fn main() {
//! let mut conn_slot: ReusableFutureRef<&'static str> = ReusableFutureRef::default();
//! let mut ticks = 0;
//!
//! loop {
//!     // Start connecting if currently idle
//!     if conn_slot.is_empty() {
//!         conn_slot.set(connect_server);
//!     }
//!
//!     tokio::select! {
//!         biased;
//!         // Poll the in-flight connection only when active
//!         res = &mut conn_slot, if !conn_slot.is_empty() => {
//!             conn_slot.reset(); // Reset to idle and reclaim buffer
//!             assert_eq!(res, "connected");
//!             break;
//!         }
//!         _ = tokio::time::sleep(Duration::from_millis(10)) => {
//!             ticks += 1;
//!             // Other event fired!
//!             // conn_slot is NOT dropped, handshake continues in the background!
//!         }
//!     }
//! }
//! assert!(ticks > 0);
//! # }
//! ```

use futures_util::future::Pending;
use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::mem::align_of;
use std::ptr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(not(feature = "triomphe"))]
use std::sync::Arc;
#[cfg(feature = "triomphe")]
use triomphe::Arc;

pub use std::future::{Future, poll_fn};
pub use std::pin::Pin;
pub use std::task::{Context, Poll};

/// Polls a future once without waking
pub fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
    let mut future = std::pin::pin!(future);
    let waker = futures_util::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    future.as_mut().poll(&mut cx)
}

/// A reference-counted future container that allows reusing the underlying allocation
/// for different futures, provided they fit within the initially allocated layout or
/// a subsequently reallocated larger layout.
/// Uses `Arc` for `Send` access.
///
/// This is useful for scenarios where futures are frequently created and dropped,
/// reducing allocation overhead. While it implements `Send` (if the future type does),
/// it is not `Sync` and should not be accessed concurrently from multiple threads.
#[derive(Clone)]
#[repr(transparent)]
pub struct ReusableFuture(Arc<UnsafeCell<Inner>>);

unsafe impl Send for ReusableFuture {}

/// A reference wrapper that holds a borrowed future within a `ReusableFuture`.
/// Implements `Future` and ensures the borrow is released when dropped.
///
/// While `ReusableFutureRef` itself is `Send` if the underlying future is `Send`,
/// it is not `Sync`.
#[must_use]
#[repr(C)]
pub struct ReusableFutureRef<'a, T> {
    /// A raw pointer to the future trait object stored in the owner's allocation.
    dyn_ptr: *mut (dyn Future<Output = T> + 'a),
    /// A clone of the owner `ReusableFuture` to manage the borrow state.
    owner: ReusableFuture,
    /// Phantom data to associate the lifetime 'a with the borrowed future type.
    _phantom: std::marker::PhantomData<&'a T>, // Use &'a T to tie the lifetime
}

unsafe impl<T: Send> Send for ReusableFutureRef<'_, T> {}

/// A reference-counted future container that allows reusing the underlying allocation
/// for different futures, provided they fit within the initially allocated layout or
/// a subsequently reallocated larger layout.
/// Uses `Rc` for single-threaded access (not `Send`).
///
/// This is useful for scenarios where futures are frequently created and dropped
/// within a single thread, reducing allocation overhead without the cost of atomic
/// operations or mutexes.
#[derive(Clone)]
#[repr(transparent)]
pub struct ReusableRcFuture(Rc<UnsafeCell<Inner>>);

/// A reference wrapper that holds a borrowed future within a `ReusableRcFuture`.
/// Implements `Future` and ensures the borrow is released when dropped. Not thread-safe.
#[must_use]
#[repr(C)]
pub struct ReusableRcFutureRef<'a, T> {
    /// A raw pointer to the future trait object stored in the owner's allocation.
    dyn_ptr: *mut (dyn Future<Output = T> + 'a),
    /// A clone of the owner `ReusableRcFuture` to manage the borrow state.
    owner: ReusableRcFuture,
    /// Phantom data to associate the lifetime 'a with the borrowed future type.
    _phantom: std::marker::PhantomData<&'a T>, // Use &'a T to tie the lifetime
}

#[cfg(any(target_pointer_width = "16", target_pointer_width = "32"))]
type MinAlignType = f64;
#[cfg(not(any(target_pointer_width = "16", target_pointer_width = "32")))]
type MinAlignType = usize;

/// Internal state holding the allocation details.
struct Inner {
    /// The layout of the currently allocated memory block.
    layout: Layout,
    /// A raw pointer to the allocated memory.
    raw_ptr: *mut u8,
    /// Flag indicating if the memory is currently borrowed to hold a future.
    borrowed: AtomicBool,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Deallocate the memory when the Inner struct is dropped.
        self.deallocate();
    }
}

impl Inner {
    // Reallocate the memory if needed.
    fn reallocate<F>(&mut self) -> *mut F {
        let layout = Layout::new::<F>();

        if layout.size() == 0 {
            return ptr::dangling_mut::<F>();
        }

        let raw_ptr: *mut F;
        if !self.raw_ptr.is_null()
            && self.layout.size() >= layout.size()
            && self.layout.align() >= layout.align()
        {
            // Reuse the existing allocation if it fits.
            raw_ptr = self.raw_ptr.cast::<F>();
        } else {
            // Deallocate the old memory if the new layout doesn't fit.
            self.deallocate();

            unsafe {
                // SAFETY: The layout is guaranteed to be valid, so no validation check is required.
                // Calculate the new layout to accommodate the new future.
                let new_layout = Layout::from_size_align_unchecked(
                    self.layout.size().max(layout.size()),
                    self.layout.align().max(layout.align()),
                );

                // SAFETY: `new_layout.size() > 0`, global allocator is used.
                let ptr = std::alloc::alloc(new_layout).cast::<F>();
                assert!(!ptr.is_null(), "Failed to allocate memory");
                raw_ptr = ptr;

                // Update the inner state with the new layout and pointer.
                self.layout = new_layout;
                self.raw_ptr = raw_ptr.cast();
            };
        }

        raw_ptr
    }

    /// Deallocates the memory pointed to by `raw_ptr` if it's valid and the layout size is greater than 0.
    #[inline]
    fn deallocate(&mut self) {
        if !self.raw_ptr.is_null() {
            // SAFETY: `raw_ptr` was allocated with `layout` using the global allocator,
            // and it hasn't been deallocated yet (since `raw_ptr` is not null).
            unsafe { std::alloc::dealloc(self.raw_ptr, self.layout) };
            self.raw_ptr = ptr::null_mut();
        }
    }

    // Mark the allocation as borrowed.
    #[inline(always)]
    fn check_and_mark_borrowed<const ATOMIC: bool>(&self) {
        // AtomicBool::swap returns the PREVIOUS value. If it was true, it's already borrowed.
        let borrowed = if ATOMIC {
            self.borrowed.swap(true, Ordering::AcqRel)
        } else {
            self.borrowed.swap(true, Ordering::Relaxed)
        };
        if borrowed {
            panic!("ReusableFuture has been borrowed");
        }
    }
}

macro_rules! impl_reusable_future {
    (
        $ReusableFuture:ident,
        $ReusableFutureRef:ident,
        $Rc:ident,
        $atomic:literal,
        $($SendBound:tt)*
    ) => {
        impl Default for $ReusableFuture {
            /// Creates a new `ReusableFuture` with a default layout for a zero-sized type.
            fn default() -> Self {
                Self::with_layout(Layout::new::<[MinAlignType; 0]>())
            }
        }

        impl $ReusableFuture {
            /// Creates a new `ReusableFuture` with a default layout for a zero-sized type.
            pub fn new() -> Self {
                Self::with_layout(Layout::new::<[MinAlignType; 0]>())
            }

            /// Creates a new `ReusableFuture` with a specific memory layout.
            ///
            /// # Arguments
            ///
            /// * `layout` - The initial memory layout to allocate.
            ///
            /// # Returns
            ///
            /// A new `ReusableFuture` instance with allocated memory according to `layout`.
            pub fn with_layout(layout: Layout) -> Self {
                let mut raw_ptr: *mut MinAlignType = ptr::null_mut();
                let effective_layout: Layout;

                unsafe {
                    // SAFETY: The layout is guaranteed to be valid, so no validation check is required.
                    // Adjust the layout to ensure it meets the minimum alignment requirements.
                    effective_layout = Layout::from_size_align_unchecked(
                        layout.size(),
                        layout.align().max(align_of::<MinAlignType>()),
                    );

                    if effective_layout.size() > 0 {
                        // SAFETY: `effective_layout.size() > 0`, global allocator is used.
                        raw_ptr = std::alloc::alloc(effective_layout).cast::<MinAlignType>();
                        assert!(!raw_ptr.is_null(), "Failed to allocate memory");
                    }
                }

                Self($Rc::new(UnsafeCell::new(Inner {
                    layout: effective_layout,
                    raw_ptr: raw_ptr.cast(),
                    borrowed: AtomicBool::new(false),
                })))
            }

            /// Borrows the allocated memory to store and run a new future.
            ///
            /// If the new future's layout doesn't fit the current allocation, the old memory
            /// is deallocated, and new memory is allocated.
            ///
            /// # Arguments
            ///
            /// * `future_creator` - A closure that creates the future to be stored.
            ///   The created future must live at least as long as the borrow (`'a`).
            ///
            /// # Returns
            ///
            /// A `ReusableFutureRef` which holds a reference to the created future
            /// and implements the `Future` trait. The `ReusableFutureRef` ensures the
            /// borrow is released when it's dropped.
            ///
            /// # Panics
            ///
            /// Panics if the `ReusableFuture` is already borrowed.
            /// Panics if memory allocation fails.
            #[inline(always)]
            pub fn borrow<'a, F, T, C>(self, future_creator: C) -> $ReusableFutureRef<'a, T>
            where
                F: Future<Output = T> + 'a + $($SendBound)*,
                C: FnOnce() -> F,
            {
                let raw_ptr: *mut F;

                unsafe {
                    // SAFETY: Exclusive access is guaranteed by `check_and_mark_borrowed`.
                    let inner = self.as_mut();
                    inner.check_and_mark_borrowed::<$atomic>();

                    // Reallocate the memory if needed.
                    raw_ptr = inner.reallocate::<F>();

                    // SAFETY: `raw_ptr` points to a valid memory block (either newly allocated, reused,
                    // or a properly aligned dangling pointer for zero-sized types).
                    // `ptr::write` overwrites the memory without reading or dropping old content.
                    ptr::write(raw_ptr, future_creator());
                }

                $ReusableFutureRef {
                    owner: self,
                    // Cast the concrete future pointer to a trait object pointer.
                    dyn_ptr: raw_ptr as *mut dyn Future<Output = T>,
                    _phantom: std::marker::PhantomData,
                }
            }

            /// Borrows the container in an idle/empty state without initializing a future immediately.
            ///
            /// The returned [`$ReusableFutureRef`] will be in an empty state (where
            /// [`is_empty()`](Self::is_empty) returns `true`) and yields [`Poll::Pending`]
            /// when polled until a concrete future is assigned via [`set()`](Self::set).
            ///
            /// This allows pre-acquiring or reserving the future slot without incurring the
            /// cost of constructing a dummy future or triggering redundant heap allocations.
            ///
            /// # Returns
            ///
            /// A new reference wrapper in the idle/empty state. The borrow is held until
            /// the reference is dropped.
            ///
            /// # Panics
            ///
            /// Panics if the container is already borrowed.
            pub fn borrow_empty<'a, T>(self) -> $ReusableFutureRef<'a, T> {
                unsafe {
                    // SAFETY: Exclusive access is guaranteed by `check_and_mark_borrowed`.
                    self.as_mut().check_and_mark_borrowed::<$atomic>();
                }

                $ReusableFutureRef {
                    owner: self,
                    dyn_ptr: ptr::null_mut::<Pending<T>>() as *mut dyn Future<Output = T>,
                    _phantom: std::marker::PhantomData,
                }
            }

            /// # Safety
            ///
            /// This method returns a mutable reference from a shared reference.
            /// It is only safe to use if the caller ensures that no other mutable or shared
            /// references to the `Inner` data exist.
            #[allow(clippy::mut_from_ref)]
            #[inline(always)]
            unsafe fn as_mut(&self) -> &mut Inner { unsafe {
                &mut *self.0.get()
            }}
        }

        impl<T> Unpin for $ReusableFutureRef<'_, T> {}

        impl<T> $ReusableFutureRef<'_, T> {
            /// Check if the underlying future is in an idle/empty state.
            ///
            /// Uses a null data-pointer sentinel to distinguish the reset/empty state
            /// from any active future (including custom zero-sized futures) with zero overhead.
            #[inline(always)]
            pub fn is_empty(&self) -> bool {
                self.dyn_ptr.cast::<()>().is_null()
            }

            /// Resets the future to idle/empty.
            ///
            /// This effectively resets the current future and releases any resources
            /// held by it, setting it to the idle state.
            pub fn reset(&mut self) {
                if !self.is_empty() {
                    // SAFETY: `dyn_ptr` points to a valid, initialized `Future` instance.
                    unsafe {
                        ptr::drop_in_place(self.dyn_ptr);
                        self.dyn_ptr = ptr::null_mut::<Pending<T>>() as *mut dyn Future<Output = T>;
                    }
                }
            }
        }

        impl<T> Drop for $ReusableFutureRef<'_, T> {
            fn drop(&mut self) {
                // Drop the internal future if it exists.
                self.reset();

                unsafe {
                    // SAFETY: mutable `self` holds the unique reference to the owner's inner state.
                    // Release the borrow on the owner's inner state.
                    let inner = self.owner.as_mut();
                    if $atomic {
                        inner.borrowed.store(false, Ordering::Release);
                    } else {
                        inner.borrowed.store(false, Ordering::Relaxed);
                    }
                }
            }
        }

        impl<T> Future for $ReusableFutureRef<'_, T> {
            type Output = T;

            /// Polls the underlying future.
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
                let this = Pin::into_inner(self);

                if this.is_empty() {
                    return Poll::Pending;
                }

                // SAFETY: `dyn_ptr` points to a valid `Future` instance. The `ReusableFutureRef`
                // ensures the data pointed to by `dyn_ptr` is valid for the lifetime of `self`.
                // Pinning `dyn_ptr` is safe because the `ReusableFutureRef` itself is pinned,
                // and the underlying `ReusableFuture` guarantees the allocation won't move
                // while borrowed. The future `F` stored within the allocation is the one being polled.
                let pin = unsafe { Pin::new_unchecked(&mut *this.dyn_ptr) };
                // Poll the pinned future.
                pin.poll(cx)
            }
        }

        impl<T> Default for $ReusableFutureRef<'_, T>
        where
            T: $($SendBound)*,
        {
            fn default() -> Self {
                $ReusableFuture::default().borrow_empty()
            }
        }

        impl<'a, T> $ReusableFutureRef<'a, T>
        where
            T: $($SendBound)*,
        {
            /// Creates a new `ReusableFutureRef` with a future.
            ///
            /// # Arguments
            /// * `future_creator` - A closure that creates the future to be stored.
            ///   The created future must live at least as long as the borrow (`'a`).
            ///
            /// # Returns
            /// * A new `ReusableFutureRef` instance.
            ///
            /// # Panics
            /// * If memory allocation fails.
            #[inline(always)]
            pub fn new<F, C>(future_creator: C) -> Self
            where
                F: Future<Output = T> + 'a + $($SendBound)*,
                C: FnOnce() -> F,
            {
                $ReusableFuture::with_layout(Layout::new::<F>()).borrow(future_creator)
            }

            /// Set a new future for the `ReusableFutureRef`.
            ///
            /// # Arguments
            /// * `future_creator` - A closure that creates the future to be stored.
            ///   The created future must live at least as long as the borrow (`'a`).
            ///
            /// # Panics
            /// * If memory allocation fails.
            ///
            #[inline(always)]
            pub fn set<F, C>(&mut self, future_creator: C)
            where
                F: Future<Output = T> + 'a + $($SendBound)*,
                C: FnOnce() -> F,
            {
                // Drop the internal future if it exists.
                self.reset();

                unsafe {
                    // SAFETY: mutable `self` holds the unique reference to the owner's inner state.
                    // Reallocate the memory if needed.
                    let raw_ptr = self.owner.as_mut().reallocate::<F>();

                    // SAFETY: `raw_ptr` points to a valid memory block (either newly allocated, reused,
                    // or a properly aligned dangling pointer for zero-sized types).
                    // `ptr::write` overwrites the memory without reading or dropping the old content.
                    ptr::write(raw_ptr, future_creator());

                    self.dyn_ptr = raw_ptr as *mut dyn Future<Output = T>;
                }
            }

            /// Drop the `ReusableFutureRef` but reclaim the allocated memory.
            ///
            /// # Returns
            ///
            /// The `ReusableFuture` instance which holds the allocated (reusable) memory.
            pub fn reclaim(self) -> $ReusableFuture {
                self.owner.clone()
            }
        }
    };
}

impl_reusable_future!(ReusableFuture, ReusableFutureRef, Arc, true, Send);
impl_reusable_future!(ReusableRcFuture, ReusableRcFutureRef, Rc, false,);
