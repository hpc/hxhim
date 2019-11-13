#include <algorithm>
#include <climits>
#include <random>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

// test C and C++ at the same time
#include "utils/elen.h"

// wrapper to clean up encode tests
#define ELEN_EQ(elen, expected)                 \
    {                                           \
        char * str = elen;                      \
        EXPECT_STREQ(str, expected);            \
        free(str);                              \
    }                                           \

// Efficient Lexicographic Encoding of Numbers
// Peter Seymour
// https://github.com/jordanorelli/lexnum

// Chapter 3 Integers
TEST(elen, encode_integers) {
    ELEN_EQ((elen_encode_int(-1234567891, '-', '+')), "---7898765432108");
    ELEN_EQ((elen_encode_int(-1234567890, '-', '+')), "---7898765432109");
    ELEN_EQ((elen_encode_int(-1234567889, '-', '+')), "---7898765432110");

    ELEN_EQ((elen_encode_int(-11,         '-', '+')), "--788");
    ELEN_EQ((elen_encode_int(-10,         '-', '+')), "--789");
    ELEN_EQ((elen_encode_int(-9,          '-', '+')), "-0");

    ELEN_EQ((elen_encode_int(-2,          '-', '+')), "-7");
    ELEN_EQ((elen_encode_int(-1,          '-', '+')), "-8");
    ELEN_EQ((elen_encode_int(0,           '-', '+')), "0");
    ELEN_EQ((elen_encode_int(+1,          '-', '+')), "+1");
    ELEN_EQ((elen_encode_int(+2,          '-', '+')), "+2");

    ELEN_EQ((elen_encode_int(+9,          '-', '+')), "+9");
    ELEN_EQ((elen_encode_int(+10,         '-', '+')), "++210");
    ELEN_EQ((elen_encode_int(+11,         '-', '+')), "++211");

    ELEN_EQ((elen_encode_int(+1234567889, '-', '+')), "+++2101234567889");
    ELEN_EQ((elen_encode_int(+1234567890, '-', '+')), "+++2101234567890");
    ELEN_EQ((elen_encode_int(+1234567891, '-', '+')), "+++2101234567891");
}

// Chapter 3 Integers
TEST(elen, decode_integers) {
    EXPECT_EQ((elen_decode_int("---7898765432108", NULL, '-', '+')), -1234567891);
    EXPECT_EQ((elen_decode_int("---7898765432109", NULL, '-', '+')), -1234567890);
    EXPECT_EQ((elen_decode_int("---7898765432110", NULL, '-', '+')), -1234567889);

    EXPECT_EQ((elen_decode_int("--788",            NULL, '-', '+')), -11);
    EXPECT_EQ((elen_decode_int("--789",            NULL, '-', '+')), -10);
    EXPECT_EQ((elen_decode_int("-0",               NULL, '-', '+')), -9);

    EXPECT_EQ((elen_decode_int("-7",               NULL, '-', '+')), -2);
    EXPECT_EQ((elen_decode_int("-8",               NULL, '-', '+')), -1 );
    EXPECT_EQ((elen_decode_int("0",                NULL, '-', '+')), 0);
    EXPECT_EQ((elen_decode_int("+1",               NULL, '-', '+')), 1);
    EXPECT_EQ((elen_decode_int("+2",               NULL, '-', '+')), 2);

    EXPECT_EQ((elen_decode_int("+9",               NULL, '-', '+')), 9);
    EXPECT_EQ((elen_decode_int("++210",            NULL, '-', '+')), 10);
    EXPECT_EQ((elen_decode_int("++211",            NULL, '-', '+')), 11);

    EXPECT_EQ((elen_decode_int("+++2101234567889", NULL, '-', '+')), +1234567889);
    EXPECT_EQ((elen_decode_int("+++2101234567890", NULL, '-', '+')), +1234567890);
    EXPECT_EQ((elen_decode_int("+++2101234567891", NULL, '-', '+')), +1234567891);
}

// Chapter 3 Integers
TEST(elen, random_integers) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(INT_MIN, INT_MAX);

    std::vector<int> ints;
    std::vector<char *> strings;
    for(std::size_t i = 0; i < 10; i++) {
        const int value = dist(gen);
        ints.push_back(value);
        strings.push_back(elen_encode_int(value, ELEN_NEG, ELEN_POS));
    }

    std::sort(ints.begin(), ints.end());
    std::sort(strings.begin(), strings.end(), lex_comp);

    std::vector<int>::const_iterator i_it = ints.begin();
    std::vector<char *>::const_iterator s_it = strings.begin();
    while (i_it != ints.end()) {
        EXPECT_EQ(*i_it, (elen_decode_int(*s_it, NULL, ELEN_NEG, ELEN_POS)));
        free(*s_it);
        i_it++;
        s_it++;
    }
}

// Chapter 4 Small Decimals
TEST(elen, encode_small_decimals) {
    ELEN_EQ((elen_encode_small_float(-0.9995, 4,       '-', '+')),    "-0004+");
    ELEN_EQ((elen_encode_small_float(-0.999, 3,        '-', '+')),     "-000+");
    ELEN_EQ((elen_encode_small_float(-0.0123, 4,       '-', '+')),    "-9876+");
    ELEN_EQ((elen_encode_small_float(-0.00123, 5,      '-', '+')),   "-99876+");
    ELEN_EQ((elen_encode_small_float(-0.0001233, 7,    '-', '+')), "-9998766+");
    ELEN_EQ((elen_encode_small_float(-0.000123, 6,     '-', '+')),  "-999876+");
    ELEN_EQ((elen_encode_small_float(0, 0,             '-', '+')),         "0");
    ELEN_EQ((elen_encode_small_float(+0.000123, 6,     '-', '+')),  "+000123-");
    ELEN_EQ((elen_encode_small_float(+0.0001233, 7,    '-', '+')), "+0001233-");
    ELEN_EQ((elen_encode_small_float(+0.00123, 5,      '-', '+')),   "+00123-");
    ELEN_EQ((elen_encode_small_float(+0.0123, 4,       '-', '+')),    "+0123-");
    ELEN_EQ((elen_encode_small_float(+0.999, 3,        '-', '+')),     "+999-");
    ELEN_EQ((elen_encode_small_float(+0.9995, 4,       '-', '+')),    "+9995-");
}

// Chapter 4 Small Decimals
TEST(elen, decode_small_decimals) {
    EXPECT_FLOAT_EQ((elen_decode_small_float("-0004+",      '-', '+')),    -0.9995);
    EXPECT_FLOAT_EQ((elen_decode_small_float("-000+",       '-', '+')),     -0.999);
    EXPECT_FLOAT_EQ((elen_decode_small_float("-9876+",      '-', '+')),    -0.0123);
    EXPECT_FLOAT_EQ((elen_decode_small_float("-99876+",     '-', '+')),   -0.00123);
    EXPECT_FLOAT_EQ((elen_decode_small_float("-9998766+",   '-', '+')), -0.0001233);
    EXPECT_FLOAT_EQ((elen_decode_small_float("-999876+",    '-', '+')),  -0.000123);
    EXPECT_FLOAT_EQ((elen_decode_small_float("0",           '-', '+')),          0);
    EXPECT_FLOAT_EQ((elen_decode_small_float("+000123-",    '-', '+')),  +0.000123);
    EXPECT_FLOAT_EQ((elen_decode_small_float("+0001233-",   '-', '+')), +0.0001233);
    EXPECT_FLOAT_EQ((elen_decode_small_float("+00123-",     '-', '+')),   +0.00123);
    EXPECT_FLOAT_EQ((elen_decode_small_float("+0123-",      '-', '+')),    +0.0123);
    EXPECT_FLOAT_EQ((elen_decode_small_float("+999-",       '-', '+')),     +0.999);
    EXPECT_FLOAT_EQ((elen_decode_small_float("+9995-",      '-', '+')),    +0.9995);
}

// Chapter 4 Small Decimals
TEST(elen, random_small_decimals) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dist(-1, 1);

    std::vector<float> floats;
    std::vector<char *> strings;
    for(std::size_t i = 0; i < 10; i++) {
        const float value = dist(gen);
        floats.push_back(value);
        strings.push_back(elen_encode_small_float(value, 2 * sizeof(float), ELEN_NEG, ELEN_POS));
    }

    std::sort(floats.begin(), floats.end());
    std::sort(strings.begin(), strings.end(), lex_comp);

    std::vector<float>::const_iterator f_it = floats.begin();
    std::vector<char *>::const_iterator s_it = strings.begin();
    while (f_it != floats.end()) {
        EXPECT_FLOAT_EQ(*f_it, (elen_decode_small_float(*s_it, ELEN_NEG, ELEN_POS)));
        free(*s_it);
        f_it++;
        s_it++;
    }
}

// Chapter 5 Large Decimals
TEST(elen, encode_large_decimals) {
    ELEN_EQ((elen_encode_large_float(-100.5,     4,   '-', '+')),   "--68994+");
    ELEN_EQ((elen_encode_large_float(-10.5,      3,   '-', '+')),    "--7894+");
    ELEN_EQ((elen_encode_large_float(-3.145,     4,   '-', '+')),     "-6854+"); // -3854+
    ELEN_EQ((elen_encode_large_float(-3.14,      3,   '-', '+')),      "-685+"); // -385+
    ELEN_EQ((elen_encode_large_float(-1.01,      3,   '-', '+')),      "-898+"); // -198+
    ELEN_EQ((elen_encode_large_float(-1,         1,   '-', '+')),        "-8+"); // -1+
    ELEN_EQ((elen_encode_large_float(-0.0001233, 7,   '-', '+')), "-99998766+"); // -09998766+
    ELEN_EQ((elen_encode_large_float(-0.000123,  6,   '-', '+')),  "-9999876+"); // -0999876+
    ELEN_EQ((elen_encode_large_float(0,          0,   '-', '+')),          "0");
    ELEN_EQ((elen_encode_large_float(+0.000123,  6,   '-', '+')),  "+0000123-");
    ELEN_EQ((elen_encode_large_float(+0.0001233, 7,   '-', '+')), "+00001233-");
    ELEN_EQ((elen_encode_large_float(+1,         1,   '-', '+')),        "+1-");
    ELEN_EQ((elen_encode_large_float(+1.01,      3,   '-', '+')),      "+101-");
    ELEN_EQ((elen_encode_large_float(+3.14,      3,   '-', '+')),      "+314-");
    ELEN_EQ((elen_encode_large_float(+3.145,     4,   '-', '+')),     "+3145-");
    ELEN_EQ((elen_encode_large_float(+10.5,      3,   '-', '+')),    "++2105-");
    ELEN_EQ((elen_encode_large_float(+100.5,     4,   '-', '+')),   "++31005-");
}

// Chapter 5 Large Decimals
TEST(elen, decode_large_decimals) {
    EXPECT_FLOAT_EQ((elen_decode_large_float("--68994+",   '-', '+')),     -100.5);
    EXPECT_FLOAT_EQ((elen_decode_large_float("--7894+",    '-', '+')),      -10.5);
    EXPECT_FLOAT_EQ((elen_decode_large_float("-6854+",     '-', '+')),     -3.145);   // -3854+
    EXPECT_FLOAT_EQ((elen_decode_large_float("-685+",      '-', '+')),      -3.14);   // -385+
    EXPECT_FLOAT_EQ((elen_decode_large_float("-898+",      '-', '+')),      -1.01);   // -198+
    EXPECT_FLOAT_EQ((elen_decode_large_float("-8+",        '-', '+')),         -1);   // -1+
    EXPECT_FLOAT_EQ((elen_decode_large_float("-99998766+", '-', '+')), -0.0001233);   // -09998766+
    EXPECT_FLOAT_EQ((elen_decode_large_float("-9999876+",  '-', '+')),  -0.000123);   // -0999876+
    EXPECT_FLOAT_EQ((elen_decode_large_float("0",          '-', '+')),          0);
    EXPECT_FLOAT_EQ((elen_decode_large_float("+0000123-",  '-', '+')),  +0.000123);
    EXPECT_FLOAT_EQ((elen_decode_large_float("+00001233-", '-', '+')), +0.0001233);
    EXPECT_FLOAT_EQ((elen_decode_large_float("+1-",        '-', '+')),         +1);
    EXPECT_FLOAT_EQ((elen_decode_large_float("+101-",      '-', '+')),      +1.01);
    EXPECT_FLOAT_EQ((elen_decode_large_float("+314-",      '-', '+')),      +3.14);
    EXPECT_FLOAT_EQ((elen_decode_large_float("+3145-",     '-', '+')),     +3.145);
    EXPECT_FLOAT_EQ((elen_decode_large_float("++2105-",    '-', '+')),      +10.5);
    EXPECT_FLOAT_EQ((elen_decode_large_float("++31005-",   '-', '+')),     +100.5);
}

// Chapter 5 Large Decimals
TEST(elen, random_large_decimals) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dist(-100, 100);

    std::vector<float> floats;
    std::vector<char *> strings;
    for(std::size_t i = 0; i < 10; i++) {
        const float value = dist(gen);
        floats.push_back(value);
        strings.push_back(elen_encode_large_float(value, 2 * sizeof(float), ELEN_NEG, ELEN_POS));
    }

    std::sort(floats.begin(), floats.end());
    std::sort(strings.begin(), strings.end(), lex_comp);

    std::vector<float>::const_iterator f_it = floats.begin();
    std::vector<char *>::const_iterator s_it = strings.begin();
    while (f_it != floats.end()) {
        EXPECT_FLOAT_EQ(*f_it, (elen_decode_large_float(*s_it, ELEN_NEG, ELEN_POS)));
        free(*s_it);
        f_it++;
        s_it++;
    }
}

// Chapter 6 Floating Pointer Numbers
TEST(elen, encode_floating_point) {
    ELEN_EQ((elen_encode_floating_float(-0.1e11,   1,     '-', '+')),  "---7888+");
    ELEN_EQ((elen_encode_floating_float(-0.1e10,   1,     '-', '+')),  "---7898+");
    ELEN_EQ((elen_encode_floating_float(-1.4,      2,     '-', '+')),    "--885+");
    ELEN_EQ((elen_encode_floating_float(-1.3,      2,     '-', '+')),    "--886+");
    ELEN_EQ((elen_encode_floating_float(-1,        1,     '-', '+')),     "--88+");
    ELEN_EQ((elen_encode_floating_float(-0.123,    4,     '-', '+')),    "-0876+");
    ELEN_EQ((elen_encode_floating_float(-0.0123,   4,     '-', '+')),   "-+1876+");
    ELEN_EQ((elen_encode_floating_float(-0.001233, 6,     '-', '+')),  "-+28766+");
    ELEN_EQ((elen_encode_floating_float(-0.00123,  5,     '-', '+')),   "-+2876+");
    ELEN_EQ((elen_encode_floating_float(0,         0,     '-', '+')),         "0");
    ELEN_EQ((elen_encode_floating_float(+0.00123,  5,     '-', '+')),   "+-7123-");
    ELEN_EQ((elen_encode_floating_float(+0.001233, 6,     '-', '+')),  "+-71233-");
    ELEN_EQ((elen_encode_floating_float(+0.0123,   4,     '-', '+')),   "+-8123-");
    ELEN_EQ((elen_encode_floating_float(+0.123,    3,     '-', '+')),    "+0123-");
    ELEN_EQ((elen_encode_floating_float(+1,        1,     '-', '+')),     "++11-");
    ELEN_EQ((elen_encode_floating_float(+1.3,      2,     '-', '+')),    "++113-");
    ELEN_EQ((elen_encode_floating_float(+1.4,      2,     '-', '+')),    "++114-");
    ELEN_EQ((elen_encode_floating_float(+0.1e10,   1,     '-', '+')),  "+++2101-");
    ELEN_EQ((elen_encode_floating_float(+0.1e11,   1,     '-', '+')),  "+++2111-");
}

// Chapter 6 Floating Pointer Numbers
TEST(elen, decode_floating_point) {
    EXPECT_FLOAT_EQ((elen_decode_floating_float("---7888+",    '-', '+')),   -0.1e11);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("---7898+",    '-', '+')),   -0.1e10);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("--885+",      '-', '+')),      -1.4);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("--886+",      '-', '+')),      -1.3);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("--88+",       '-', '+')),        -1);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("-0876+",      '-', '+')),    -0.123);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("-+1876+",     '-', '+')),   -0.0123);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("-+28766+",    '-', '+')), -0.001233);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("-+2876+",     '-', '+')),  -0.00123);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("0",           '-', '+')),         0);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("+-7123-",     '-', '+')),  +0.00123);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("+-71233-",    '-', '+')), +0.001233);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("+-8123-",     '-', '+')),   +0.0123);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("+0123-",      '-', '+')),    +0.123);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("++11-",       '-', '+')),        +1);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("++113-",      '-', '+')),      +1.3);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("++114-",      '-', '+')),      +1.4);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("+++2101-",    '-', '+')),   +0.1e10);
    EXPECT_FLOAT_EQ((elen_decode_floating_float("+++2111-",    '-', '+')),   +0.1e11);
}

// Chapter 6 Floating Pointer Numbers
TEST(elen, random_floating_point) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dist(-100, 100);

    std::vector<float> floats;
    std::vector<char *> strings;
    for(std::size_t i = 0; i < 10; i++) {
        const float value = dist(gen);
        floats.push_back(value);
        strings.push_back(elen_encode_floating_float(value, 2 * sizeof(float), ELEN_NEG, ELEN_POS));
    }

    std::sort(floats.begin(), floats.end());
    std::sort(strings.begin(), strings.end(), lex_comp);

    std::vector<float>::const_iterator f_it = floats.begin();
    std::vector<char *>::const_iterator s_it = strings.begin();
    while (f_it != floats.end()) {
        EXPECT_FLOAT_EQ(*f_it, (elen_decode_floating_float(*s_it, ELEN_NEG, ELEN_POS)));
        free(*s_it);
        f_it++;
        s_it++;
    }
}
