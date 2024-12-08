#pragma once
#include<functional>

class ScopeGuard {
public:
    explicit ScopeGuard(std::function<void()> func): m_func(std::move(func)) {}
    ~ScopeGuard() {
        m_func();
    }

private:
    std::function<void()> m_func;
};
