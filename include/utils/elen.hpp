#ifndef EFFICIENT_LEXICOGRAPHIC_ENCODING_OF_NUMBERS_HPP
#define EFFICIENT_LEXICOGRAPHIC_ENCODING_OF_NUMBERS_HPP

// Efficient Lexicographic Encoding of Numbers
// Peter Seymour
// https://github.com/jordanorelli/lexnum

#include <cmath>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>

#include "utils/type_traits.hpp"

// Intentionally set default precision too high for floating point values
// Do not use std::numeric_limits<T>::digits10
namespace elen {
    static const char N = '0' - 1;
    static const char P = '9' + 1;
    static_assert(N <= P, "Character used as negative sign is not less than the one used for positives");

    namespace encode {
        // Chapter 3 Integers
        template <typename T,
                  typename = enable_if_t<std::is_integral<T>::value> >
        std::string integers(const T value, const char neg = N, const char pos = P);

        // Chapter 4 Small Decimals
        template <typename T,
                  typename = enable_if_t<std::is_floating_point<T>::value> >
        std::string small_decimals(const T value, const int precision = 2 * sizeof(T), const char neg = N, const char pos = P);

        // Chapter 5 Large Decimals
        template <typename T,
                  typename = enable_if_t<std::is_floating_point<T>::value> >
        std::string large_decimals(const T value, const int precision = 2 * sizeof(T), const char neg = N, const char pos = P);

        // Chapter 6 Floating Pointer Numbers
        template <typename T,
                  typename = enable_if_t<std::is_floating_point<T>::value> >
        std::string floating_point(const T value, const int precision = 2 * sizeof(T), const char neg = N, const char pos = P);
    }

    namespace decode {
        // Chapter 3 Integers
        template <typename T,
                  typename = enable_if_t<std::is_integral<T>::value> >
        T integers(const std::string &str, std::size_t* prefix_len = nullptr, const char neg = N, const char pos = P);

        // Chapter 4 Small Decimals
        template <typename T,
                  typename = enable_if_t<std::is_floating_point<T>::value> >
        T small_decimals(const std::string &str, const char neg = N, const char pos = P);

        // Chapter 5 Large Decimals
        template <typename T,
                  typename = enable_if_t<std::is_floating_point<T>::value> >
        T large_decimals(const std::string &str, const char neg = N, const char pos = P);

        // Chapter 6 Floating Pointer Numbers
        template <typename T,
                  typename = enable_if_t<std::is_floating_point<T>::value> >
        T floating_point(const std::string &str, const char neg = N, const char pos = P);
    }
}

/* @description Use this function for lexicographic comparisions */
bool lex_comp(const std::string &lhs, const std::string &rhs);

#include "elen.tpp"

#endif
