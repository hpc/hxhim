#ifndef EFFICIENT_LEXICOGRAPHIC_ENCODING_OF_NUMBERS
#define EFFICIENT_LEXICOGRAPHIC_ENCODING_OF_NUMBERS

// Efficient Lexicographic Encoding of Numbers
// Peter Seymour
// https://github.com/jordanorelli/lexnum

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

#include "utils/enable_if_t.hpp"

// Intentionally set default precision too high for floating point values
// Do not use std::numeric_limits<T>::digits10
namespace elen {
    namespace encode {
        // Chapter 3 Integers
        template <const char neg = '0' - 1, const char pos = '9' + 1,
                  typename T,
                  typename = enable_if_t<(neg < '0') && (pos > '9') && std::is_integral<T>::value> >
        static std::string integers(const T value);

        // Chapter 4 Small Decimals
        template <const char neg = '0' - 1, const char pos = '9' + 1,
                  typename T,
                  typename = enable_if_t<(neg < '0') && (pos > '9') && std::is_floating_point<T>::value> >
        static std::string small_decimals(const T value, const int precision = 2 * sizeof(T));

        // Chapter 5 Large Decimals
        template <const char neg = '0' - 1, const char pos = '9' + 1,
                  typename T,
                  typename = enable_if_t<(neg < '0') && (pos > '9') && std::is_floating_point<T>::value> >
        static std::string large_decimals(const T value, const int precision = 2 * sizeof(T));

        // Chapter 6 Floating Pointer Numbers
        template <const char neg = '0' - 1, const char pos = '9' + 1,
                  typename T,
                  typename = enable_if_t<(neg < '0') && (pos > '9') && std::is_floating_point<T>::value> >
        static std::string floating_point(const T value, const int precision = 2 * sizeof(T));
    }

    namespace decode {
        // Chapter 3 Integers
        template <typename T,
                  const char neg = '0' - 1, const char pos = '9' + 1,
                  typename = enable_if_t<std::is_integral<T>::value && (neg < '0') && (pos > '9')> >
        static T integers(const std::string &str, std::size_t* prefix_len = nullptr);

        // Chapter 4 Small Decimals
        template <typename T,
                  const char neg = '0' - 1, const char pos = '9' + 1,
                  typename = enable_if_t<std::is_floating_point<T>::value && (neg < '0') && (pos > '9')> >
        static T small_decimals(const std::string &str);

        // Chapter 5 Large Decimals
        template <typename T,
                  const char neg = '0' - 1, const char pos = '9' + 1,
                  typename = enable_if_t<std::is_floating_point<T>::value && (neg < '0') && (pos > '9')> >
        static T large_decimals(const std::string &str);

        // Chapter 6 Floating Pointer Numbers
        template <typename T,
                  const char neg = '0' - 1, const char pos = '9' + 1,
                  typename = enable_if_t<std::is_floating_point<T>::value && (neg < '0') && (pos > '9')> >
        static T floating_point(const std::string &str);
    }
}

/* @description Use this function for lexicographic comparisions */
[[maybe_unused]] static bool lex_comp(const std::string &lhs, const std::string &rhs);

#include "elen.tpp"

#endif
