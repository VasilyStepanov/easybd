#pragma once

#include <coroutine>
#include <utility>

namespace easyio {

// Lets one coroutine wait for N independently-spawned (fire-and-forget)
// tasks to finish. Single-threaded, coroutine-only -- no OS primitives, same
// rationale as Mutex.
class WaitGroup {
public:
    void add(int n = 1) noexcept { _remaining += n; }

    void done() noexcept {
        if (--_remaining <= 0 && _waiter) {
            std::exchange(_waiter, nullptr).resume();
        }
    }

    struct Awaiter {
        WaitGroup* wg;
        [[nodiscard]] bool await_ready() const noexcept { return wg->_remaining <= 0; }
        void await_suspend(std::coroutine_handle<> h) noexcept { wg->_waiter = h; }
        void await_resume() const noexcept {}
    };

    Awaiter wait() noexcept { return Awaiter{this}; }

private:
    int _remaining = 0;
    std::coroutine_handle<> _waiter;
};

} // namespace easyio
