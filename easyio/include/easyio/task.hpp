#pragma once

#include <coroutine>
#include <exception>
#include <utility>
#include <variant>

namespace easyio {

// Minimal single-owner, lazily-started coroutine task. Modeled after the
// well-known cppcoro::task shape: awaiting a Task<T> resumes the awaiter
// when the task's coroutine body runs to completion (or throws), on
// whatever thread happens to resume the task's coroutine_handle (that is
// always a Queue's dispatch loop for tasks that co_await I/O).
template <typename T = void>
class Task;

namespace detail {

struct TaskPromiseBase {
    std::coroutine_handle<> continuation;

    struct FinalAwaiter {
        bool await_ready() const noexcept { return false; }

        template <typename Promise>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> h) noexcept {
            // Resume whoever is co_await-ing this task, if anyone; otherwise
            // this was a detached/spawned task and there is nothing to do.
            if (auto c = h.promise().continuation) {
                return c;
            }
            return std::noop_coroutine();
        }

        void await_resume() noexcept {}
    };

    std::suspend_always initial_suspend() noexcept { return {}; }
    FinalAwaiter final_suspend() noexcept { return {}; }
};

template <typename T>
struct TaskPromise final : TaskPromiseBase {
    std::variant<std::monostate, T, std::exception_ptr> result;

    Task<T> get_return_object() noexcept;

    template <typename U>
    void return_value(U&& value) {
        result.template emplace<1>(std::forward<U>(value));
    }

    void unhandled_exception() noexcept {
        result.template emplace<2>(std::current_exception());
    }

    T take_result() {
        if (result.index() == 2) {
            std::rethrow_exception(std::get<2>(result));
        }
        return std::move(std::get<1>(result));
    }
};

template <>
struct TaskPromise<void> final : TaskPromiseBase {
    std::exception_ptr exception;

    Task<void> get_return_object() noexcept;

    void return_void() noexcept {}

    void unhandled_exception() noexcept { exception = std::current_exception(); }

    void take_result() {
        if (exception) {
            std::rethrow_exception(exception);
        }
    }
};

} // namespace detail

template <typename T>
class [[nodiscard]] Task {
public:
    using promise_type = detail::TaskPromise<T>;

    Task() noexcept = default;
    explicit Task(std::coroutine_handle<promise_type> h) noexcept : _handle(h) {}

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept : _handle(std::exchange(other._handle, {})) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            destroy();
            _handle = std::exchange(other._handle, {});
        }
        return *this;
    }

    ~Task() { destroy(); }

    bool valid() const noexcept { return static_cast<bool>(_handle); }

    // Awaiter protocol: co_await task resumes the calling coroutine once
    // task's body completes.
    [[nodiscard]] bool await_ready() const noexcept { return !_handle || _handle.done(); }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiting) noexcept {
        _handle.promise().continuation = awaiting;
        return _handle;
    }

    [[nodiscard]] T await_resume() { return _handle.promise().take_result(); }

    // Drives the coroutine to its first suspension point (i.e. actually
    // starts it) without waiting for a result. Used to fire-and-forget
    // "spawn" a detached task from a Queue::accept loop.
    void start() {
        if (_handle && !_handle.done()) {
            _handle.resume();
        }
    }

    std::coroutine_handle<promise_type> handle() const noexcept { return _handle; }

private:
    void destroy() {
        if (_handle) {
            _handle.destroy();
            _handle = {};
        }
    }

    std::coroutine_handle<promise_type> _handle;
};

namespace detail {

template <typename T>
inline Task<T> TaskPromise<T>::get_return_object() noexcept {
    return Task<T>(std::coroutine_handle<TaskPromise<T>>::from_promise(*this));
}

inline Task<void> TaskPromise<void>::get_return_object() noexcept {
    return Task<void>(std::coroutine_handle<TaskPromise<void>>::from_promise(*this));
}

} // namespace detail

// Runs a detached (fire-and-forget) task to completion. Ownership of the
// coroutine frame transfers to itself: it self-destructs (via
// std::noop_coroutine substitution not being possible for a detached frame,
// we instead let FinalAwaiter's lack of continuation fall through to
// noop_coroutine and rely on the DetachedTask promise deleting itself).
struct DetachedTask {
    struct promise_type {
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        DetachedTask get_return_object() noexcept { return {}; }
        void return_void() noexcept {}
        [[noreturn]] void unhandled_exception() { std::terminate(); }
    };
};

template <typename T>
DetachedTask spawn(Task<T> task) {
    co_await std::move(task);
}

} // namespace easyio
