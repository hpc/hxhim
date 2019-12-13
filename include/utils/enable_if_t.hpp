#ifndef HXHIM_ENABLE_IF_T_HPP
#define HXHIM_ENABLE_IF_T_HPP

// including type_traits handles lack of C++11/14 support
#include <type_traits>

#if __cplusplus >= 201402L   // C++14
template <bool B, typename t = void>
using enable_if_t = std::enable_if_t<B, t>;
#elif __cplusplus >= 201103L // C++11
template <bool B, typename t = void>
using enable_if_t = typename std::enable_if<B, t>::type;
#endif

// check if Derived is a strict child of Base
template <class Base, class Derived>
struct is_child_of : public std::conditional_t <std::is_base_of <Base, Derived>::value && !std::is_same <Base, Derived>::value,
                                                std::true_type,
                                                std::false_type> {};

#endif
