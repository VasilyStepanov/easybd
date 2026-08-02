#pragma once

#include <coroutine>
#include <deque>

namespace easyio {

// A single-threaded (coroutine-only) mutual exclusion primitive. Queue is
// driven by exactly one thread, so this needs no atomics/OS synchronization
// -- it exists purely to serialize a sequence of awaits across independent
// coroutines that may otherwise interleave.
//
// The motivating case: several independently-scheduled request-handling
// coroutines on one connection each want to send a multi-part response
// (e.g. a header then a payload) on the same fd. Queue::send() may return a
// short write, so sending one logical message can take more than one
// send() call; without serialization, two in-flight responses' send()
// calls could interleave on the wire (message A's header, then message B's
// header, then message A's payload -- corrupting the framing). Wrapping
// each response's full send sequence in lock()/unlock() ensures only one
// coroutine is mid-message on a given fd at a time.
class Mutex {
public:
    struct LockAwaiter {
        Mutex* mutex;

        [[nodiscard]] bool await_ready() noexcept {
            if (!mutex->_locked) {
                mutex->_locked = true;
                return true;
            }
            return false;
        }

        void await_suspend(std::coroutine_handle<> h) noexcept { mutex->_waiters.push_back(h); }
        void await_resume() noexcept {}
    };

    LockAwaiter lock() noexcept { return LockAwaiter{this}; }

    void unlock() noexcept {
        if (!_waiters.empty()) {
            // Hand the lock directly to the next waiter -- _locked stays
            // true the whole time, so there's no window where a third
            // coroutine could race in via await_ready().
            auto h = _waiters.front();
            _waiters.pop_front();
            h.resume();
        } else {
            _locked = false;
        }
    }

private:
    bool _locked = false;
    std::deque<std::coroutine_handle<>> _waiters;
};

// RAII helper: `auto g = co_await lock_guard(mutex);` unlocks when g goes
// out of scope.
class MutexGuard {
public:
    explicit MutexGuard(Mutex& mutex) noexcept : _mutex(&mutex) {}
    MutexGuard(const MutexGuard&) = delete;
    MutexGuard& operator=(const MutexGuard&) = delete;
    MutexGuard(MutexGuard&& other) noexcept : _mutex(other._mutex) { other._mutex = nullptr; }
    ~MutexGuard() {
        if (_mutex) {
            _mutex->unlock();
        }
    }

private:
    Mutex* _mutex;
};

// Usable as `auto guard = co_await lock_guard(mutex);` -- the lock releases
// automatically when `guard` goes out of scope.
struct LockGuardAwaiter {
    Mutex* mutex;
    Mutex::LockAwaiter inner;

    explicit LockGuardAwaiter(Mutex& m) noexcept : mutex(&m), inner(m.lock()) {}

    [[nodiscard]] bool await_ready() noexcept { return inner.await_ready(); }
    void await_suspend(std::coroutine_handle<> h) noexcept { inner.await_suspend(h); }
    [[nodiscard]] MutexGuard await_resume() noexcept {
        inner.await_resume();
        return MutexGuard(*mutex);
    }
};

inline LockGuardAwaiter lock_guard(Mutex& m) noexcept { return LockGuardAwaiter{m}; }

} // namespace easyio
