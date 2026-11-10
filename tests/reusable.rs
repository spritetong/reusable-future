// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MIT OR Apache-2.0

use reusable_future::{ReusableFuture, ReusableFutureRef, ReusableRcFuture, ReusableRcFutureRef};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::test]
async fn test_reusable_future_basic() {
    let mut slot: ReusableFutureRef<i32> = ReusableFutureRef::default();
    assert!(slot.is_empty());

    // First execution
    slot.set(|| async { 10 });
    assert!(!slot.is_empty());
    assert_eq!((&mut slot).await, 10);

    // Second execution reusing memory
    slot.set(|| async { 20 });
    assert_eq!((&mut slot).await, 20);

    // Reset back to empty
    slot.reset();
    assert!(slot.is_empty());
}

#[tokio::test]
async fn test_reusable_future_grow_and_shrink() {
    let mut slot: ReusableFutureRef<usize> = ReusableFutureRef::default();

    // 1. Small future
    slot.set(|| async { 1usize });
    assert_eq!((&mut slot).await, 1);

    // 2. Large future that forces reallocate (capturing a 4KB array)
    let large_data = [42u8; 4096];
    slot.set(move || async move {
        // Access large_data to ensure it's in the state machine
        large_data.iter().map(|&x| x as usize).sum::<usize>()
    });
    assert_eq!((&mut slot).await, 42 * 4096);

    // 3. Small future again (reuses the existing 4KB+ buffer without reallocating)
    slot.set(|| async { 999usize });
    assert_eq!((&mut slot).await, 999);
}

#[tokio::test]
async fn test_drop_behavior_on_reset_and_set() {
    struct DropDetector(Arc<AtomicBool>);
    impl Drop for DropDetector {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let flag1 = Arc::new(AtomicBool::new(false));
    let flag2 = Arc::new(AtomicBool::new(false));

    let mut slot: ReusableFutureRef<()> = ReusableFutureRef::default();

    // Set first future capturing detector1
    let detector1 = DropDetector(Arc::clone(&flag1));
    slot.set(move || async move {
        let _ = &detector1;
        std::future::pending::<()>().await;
    });
    assert!(!flag1.load(Ordering::SeqCst));

    // Overwriting with set() must drop the previous future and its captured detector1
    let detector2 = DropDetector(Arc::clone(&flag2));
    slot.set(move || async move {
        let _ = &detector2;
        std::future::pending::<()>().await;
    });
    assert!(
        flag1.load(Ordering::SeqCst),
        "Old future must be dropped on set()"
    );
    assert!(!flag2.load(Ordering::SeqCst));

    // reset() must drop the current future and its captured detector2
    slot.reset();
    assert!(
        flag2.load(Ordering::SeqCst),
        "Current future must be dropped on reset()"
    );
    assert!(slot.is_empty());
}

#[tokio::test]
async fn test_select_cancellation_safety() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_clone = Arc::clone(&counter);

    // Slow async operation that takes 50ms and increments counter on completion
    let slow_task = move || {
        let counter = Arc::clone(&counter_clone);
        async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            counter.fetch_add(1, Ordering::SeqCst);
            "success"
        }
    };

    let mut slot: ReusableFutureRef<&'static str> = ReusableFutureRef::default();
    let mut other_events = 0;

    // Loop with short timeout (10ms) racing against the 50ms task
    loop {
        if slot.is_empty() {
            slot.set(slow_task.clone());
        }

        tokio::select! {
            biased;
            res = &mut slot, if !slot.is_empty() => {
                slot.reset();
                assert_eq!(res, "success");
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
                other_events += 1;
                // The 10ms timer fired first!
                // slot is NOT dropped, its 50ms sleep continues ticking!
            }
        }
    }

    // Other events should have fired several times (~4-6 times)
    assert!(
        other_events >= 2,
        "Other event fired {} times",
        other_events
    );
    // The task should have completed exactly once without being reset/aborted
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_reusable_rc_future_single_threaded() {
    let local = tokio::task::LocalSet::new();

    local
        .run_until(async {
            let mut slot: ReusableRcFutureRef<i32> = ReusableRcFutureRef::default();
            assert!(slot.is_empty());

            slot.set(|| async { 123 });
            assert_eq!((&mut slot).await, 123);

            slot.reset();
            assert!(slot.is_empty());

            let owner: ReusableRcFuture = slot.reclaim();
            let mut slot2 = owner.borrow(|| async { 456 });
            assert_eq!((&mut slot2).await, 456);
        })
        .await;
}

#[test]
fn test_reclaim_and_reborrow() {
    let slot: ReusableFutureRef<i32> = ReusableFutureRef::default();
    let owner: ReusableFuture = slot.reclaim();

    // Reborrowing from the owner
    let mut slot2 = owner.borrow(|| async { 777 });
    let res = futures::executor::block_on(&mut slot2);
    assert_eq!(res, 777);
}

#[tokio::test]
async fn test_cross_type_switching() {
    let pool = ReusableFuture::new();

    // 1. First type: i32
    let mut int_ref = pool.clone().borrow(|| async { 42i32 });
    assert_eq!((&mut int_ref).await, 42);
    drop(int_ref);

    // 2. Second type: String (completely different return type, 0 heap reallocations!)
    let mut str_ref = pool.clone().borrow(|| async { String::from("reusable") });
    assert_eq!((&mut str_ref).await, "reusable");
    drop(str_ref);

    // 3. Third type: (u64, Vec<u8>)
    let mut tuple_ref = pool.borrow(|| async { (100u64, vec![1u8, 2, 3]) });
    assert_eq!((&mut tuple_ref).await, (100u64, vec![1u8, 2, 3]));
}

struct MyZstFuture;
impl std::future::Future for MyZstFuture {
    type Output = i32;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<i32> {
        std::task::Poll::Ready(123)
    }
}

#[tokio::test]
async fn test_zst_future() {
    let mut slot: ReusableFutureRef<i32> = ReusableFutureRef::default();
    assert!(slot.is_empty());

    slot.set(|| MyZstFuture);
    assert!(
        !slot.is_empty(),
        "MyZstFuture must NOT be considered empty!"
    );
    assert_eq!((&mut slot).await, 123);
}

#[tokio::test]
async fn test_borrow_empty_behavior() {
    let pool = ReusableFuture::new();
    let mut slot: ReusableFutureRef<i32> = pool.borrow_empty();
    assert!(slot.is_empty());

    // Polling an empty slot returns Poll::Pending
    let waker = futures_util::task::noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert_eq!(
        std::pin::Pin::new(&mut slot).poll(&mut cx),
        std::task::Poll::Pending
    );

    // Setting a future transitions it to active
    slot.set(|| async { 42 });
    assert!(!slot.is_empty());
    assert_eq!((&mut slot).await, 42);

    // Reset transitions it back to empty
    slot.reset();
    assert!(slot.is_empty());

    // Reclaim returns ownership to the pool and enables new borrows
    let owner = slot.reclaim();
    let mut slot2: ReusableFutureRef<String> = owner.borrow_empty();
    assert!(slot2.is_empty());
    slot2.set(|| async { String::from("reclaimed") });
    assert_eq!((&mut slot2).await, "reclaimed");
}

#[tokio::test]
async fn test_zst_future_evaluation_and_drop() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    static ZST_DROPPED: AtomicUsize = AtomicUsize::new(0);

    struct ZstDropFuture;

    impl std::future::Future for ZstDropFuture {
        type Output = ();
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            std::task::Poll::Ready(())
        }
    }

    impl Drop for ZstDropFuture {
        fn drop(&mut self) {
            ZST_DROPPED.fetch_add(1, Ordering::SeqCst);
        }
    }

    assert_eq!(std::mem::size_of::<ZstDropFuture>(), 0);

    let created = Arc::new(AtomicBool::new(false));
    let created_clone = Arc::clone(&created);

    let pool = ReusableFuture::new();
    // Test borrow with ZST on an unallocated pool: closure must run!
    let mut slot = pool.borrow(move || {
        created_clone.store(true, Ordering::SeqCst);
        ZstDropFuture
    });

    assert!(
        created.load(Ordering::SeqCst),
        "ZST closure must be evaluated"
    );
    assert!(!slot.is_empty());
    assert_eq!(ZST_DROPPED.load(Ordering::SeqCst), 0);

    // Polling completes the future
    (&mut slot).await;

    // Reset drops the future
    slot.reset();
    assert_eq!(ZST_DROPPED.load(Ordering::SeqCst), 1);
    assert!(slot.is_empty());

    // Test slot.set with ZST on an empty slot
    let set_created = Arc::new(AtomicBool::new(false));
    let set_created_clone = Arc::clone(&set_created);
    slot.set(move || {
        set_created_clone.store(true, Ordering::SeqCst);
        ZstDropFuture
    });
    assert!(
        set_created.load(Ordering::SeqCst),
        "ZST closure in set() must be evaluated"
    );
    assert!(!slot.is_empty());

    drop(slot);
    assert_eq!(ZST_DROPPED.load(Ordering::SeqCst), 2);
}
