#ifndef HELPER_MACROS_HPP
#define HELPER_MACROS_HPP

#include <type_traits>

// std::remove_reference alias
#define REF(ref) typename std::remove_reference<decltype(ref)>::type

#endif
