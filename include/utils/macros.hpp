#ifndef HELPER_MACROS
#define HELPER_MACROS

#include <type_traits>

// std::remove_reference alias
#define REF(ref) typename std::remove_reference<decltype(ref)>::type

#endif
