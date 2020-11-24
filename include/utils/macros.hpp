#ifndef HELPER_MACROS_HPP
#define HELPER_MACROS_HPP

#include <type_traits>

#include "utils/macros.h"

// std::remove_reference alias
#define REF(ref) typename std::remove_reference<decltype(ref)>::type

#endif
