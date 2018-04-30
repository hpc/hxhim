#ifndef EFFICIENT_LEXICOGRAPHIC_ENCODING_OF_NUMBERS
#define EFFICIENT_LEXICOGRAPHIC_ENCODING_OF_NUMBERS

// Efficient Lexicographic Encoding of Numbers
// Peter Seymour
// https://github.com/jordanorelli/lexnum

#include <cmath>
#include <stdexcept>
#include <string>
// #include <string_view>
#include <type_traits>

namespace elen {
    namespace encode {
        // Chapter 3 Integers
        template <typename T, typename = std::enable_if<std::is_integral<T>::value> >
        std::string integers(const T value, const char pos = '+', const char neg = '-');

        // Chapter 4 Small Decimals
        // value is in (0, 1)
        template <typename T, typename = std::enable_if<std::is_floating_point<T>::value> >
        std::string small_decimals(const T value, const int precision, const char pos = '+', const char neg = '-');

        // Chapter 5 Large Decimals
        template <typename T, typename = std::enable_if<std::is_floating_point<T>::value> >
        std::string large_decimals(const T value, const int precision, const char pos = '+', const char neg = '-');

        // Chapter 6 Floating Pointer Numbers
        template <typename T, typename = std::enable_if<std::is_floating_point<T>::value> >
        std::string floating_point(const T value, const int precision, const char pos = '+', const char neg = '-');
    }

    namespace decode {
        // Chapter 3 Integers
        template <typename T, typename = std::enable_if<std::is_integral<T>::value> >
        T integers(const std::string &str, const char pos = '+', const char neg = '-');

        // Chapter 4 Small Decimals
        // value is in (0, 1)
        template <typename T, typename = std::enable_if<std::is_floating_point<T>::value> >
        T small_decimals(const std::string &str, const char pos = '+', const char neg = '-');

        // Chapter 5 Large Decimals
        template <typename T, typename = std::enable_if<std::is_floating_point<T>::value> >
        T large_decimals(const std::string &str, const char pos = '+', const char neg = '-');

        // Chapter 6 Floating Pointer Numbers
        template <typename T, typename = std::enable_if<std::is_floating_point<T>::value> >
        T floating_point(const std::string &str, const char pos = '+', const char neg = '-');
    }
};

#include "elen.cpp"

#endif
