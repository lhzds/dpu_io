#pragma once

#include <functional>
#include <memory>

template <typename Ret, typename... Args>
class CallonceFn;

template <typename Ret, typename... Args>
class CallonceFn<Ret(*)(Args...)> {
public:
    using FuncPtr = Ret(*)(Args...);

    static CallonceFn *make(FuncPtr func) {
        return new CallonceFn(func);
    }

    Ret operator()(Args... args) const {
        if constexpr (std::is_void<Ret>::value) {
            auto ret =  m_func(args...);
            delete this;
            return ret;
        } else {
            m_func(args...);
            delete this;
        }
    }

private:
    CallonceFn(FuncPtr func) : m_func(func) {}

    FuncPtr  m_func;
};

template <typename Ret, typename... Args>
class CallonceFn<std::function<Ret(Args...)>> {
public:
    using StdFunc = std::function<Ret(Args...)>;

    static CallonceFn *make(StdFunc func) {
        return new CallonceFn(std::move(func));
    }

    Ret operator()(Args... args) const {
        if constexpr (std::is_void<Ret>::value) {
            m_heap_func->operator()(args...);
            delete this;
        } else {
            Ret ret = m_heap_func->operator()(args...);
            delete this;
            return ret;
        }
    }

private:
    CallonceFn(StdFunc func) : m_heap_func(new StdFunc(std::move(func))) {}

    std::unique_ptr<StdFunc> m_heap_func;
};
