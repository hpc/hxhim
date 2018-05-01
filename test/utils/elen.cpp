#include <climits>
#include <random>
#include <type_traits>
#include <vector>

#include <gtest/gtest.h>

#include "elen.hpp"

// Efficient Lexicographic Encoding of Numbers
// Peter Seymour
// https://github.com/jordanorelli/lexnum

// Chapter 3 Integers
TEST(elen, encode_integers) {
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(-1234567891)), "---7898765432108");
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(-1234567890)), "---7898765432109");
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(-1234567889)), "---7898765432110");

    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(-11)), "--788");
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(-10)), "--789");
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(-9)), "-0");

    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(-2)), "-7");
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(-1)), "-8");
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(0)), "0");
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(+1)), "+1");
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(+2)), "+2");

    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(+9)), "+9");
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(+10)), "++210");
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(+11)), "++211");

    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(+1234567889)), "+++2101234567889");
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(+1234567890)), "+++2101234567890");
    EXPECT_EQ((elen::encode::integers<'-', '+', int, std::true_type>(+1234567891)), "+++2101234567891");
}

// Chapter 3 Integers
TEST(elen, decode_integers) {
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("---7898765432108")), -1234567891);
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("---7898765432109")), -1234567890);
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("---7898765432110")), -1234567889);

    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("--788")), -11);
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("--789")), -10);
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("-0")), -9);

    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("-7")), -2);
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("-8")), -1 );
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("0")), 0);
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("+1")), 1);
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("+2")), 2);

    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("+9")), 9);
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("++210")), 10);
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("++211")), 11);

    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("+++2101234567889")), +1234567889);
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("+++2101234567890")), +1234567890);
    EXPECT_EQ((elen::decode::integers<int, '-', '+', std::true_type>("+++2101234567891")), +1234567891);
}

// Chapter 3 Integers
TEST(elen, random_integers) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(INT_MIN, INT_MAX);

    std::vector<int> ints;
    std::vector<std::string> strings;
    for(std::size_t i = 0; i < 10; i++) {
        const int value = dist(gen);
        ints.push_back(value);
        strings.push_back(elen::encode::integers(value));
    }

    std::sort(ints.begin(), ints.end());
    std::sort(strings.begin(), strings.end(), lex_comp);

    std::vector<int>::const_iterator i_it = ints.begin();
    std::vector<std::string>::const_iterator s_it = strings.begin();
    while (i_it != ints.end()) {
        EXPECT_EQ(*i_it, (elen::decode::integers<int>(*s_it)));
        i_it++;
        s_it++;
    }
}

// Chapter 4 Small Decimals
TEST(elen, encode_small_decimals) {
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(-0.9995, 4)),    "-0004+");
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(-0.999, 3)),     "-000+");
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(-0.0123, 4)),    "-9876+");
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(-0.00123, 5)),   "-99876+");
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(-0.0001233, 7)), "-9998766+");
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(-0.000123, 6)),  "-999876+");
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(0, 0)),          "0");
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(+0.000123, 6)),  "+000123-");
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(+0.0001233, 7)), "+0001233-");
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(+0.00123, 5)),   "+00123-");
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(+0.0123, 4)),    "+0123-");
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(+0.999, 3)),     "+999-");
    EXPECT_EQ((elen::encode::small_decimals<'-', '+', float, std::true_type>(+0.9995, 4)),    "+9995-");
}

// Chapter 4 Small Decimals
TEST(elen, decode_small_decimals) {
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("-0004+")),    -0.9995);
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("-000+")),     -0.999);
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("-9876+")),    -0.0123);
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("-99876+")),   -0.00123);
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("-9998766+")), -0.0001233);
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("-999876+")),  -0.000123);
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("0")),          0);
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("+000123-")),  +0.000123);
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("+0001233-")), +0.0001233);
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("+00123-")),   +0.00123);
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("+0123-")),    +0.0123);
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("+999-")),     +0.999);
    EXPECT_FLOAT_EQ((elen::decode::small_decimals<float, '-', '+', std::true_type>("+9995-")),    +0.9995);
}

// Chapter 4 Small Decimals
TEST(elen, random_small_decimals) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dist(-1, 1);

    std::vector<float> floats;
    std::vector<std::string> strings;
    for(std::size_t i = 0; i < 10; i++) {
        const float value = dist(gen);
        floats.push_back(value);
        strings.push_back(elen::encode::small_decimals(value));
    }

    std::sort(floats.begin(), floats.end());
    std::sort(strings.begin(), strings.end(), lex_comp);

    std::vector<float>::const_iterator f_it = floats.begin();
    std::vector<std::string>::const_iterator s_it = strings.begin();
    while (f_it != floats.end()) {
        EXPECT_FLOAT_EQ(*f_it, (elen::decode::small_decimals<float>(*s_it)));
        f_it++;
        s_it++;
    }
}

// Chapter 5 Large Decimals
TEST(elen, encode_large_decimals) {
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(-100.5, 4)),     "--68994+");
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(-10.5, 3)),      "--7894+");
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(-3.145, 4)),     "-6854+"); // -3854+
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(-3.14, 3)),      "-685+");  // -385+
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(-1.01, 3)),      "-898+");  // -198+
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(-1, 1)),         "-8+");    // -1+
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(-0.0001233, 7)), "-09998766+");
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(-0.000123, 6)),  "-0999876+");
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(0, 0)),           "0");
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(+0.000123, 6)),  "+0000123-");
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(+0.0001233, 7)), "+00001233-");
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(+1, 1)),         "+1-");
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(+1.01, 3)),      "+101-");
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(+3.14, 3)),      "+314-");
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(+3.145, 4)),     "+3145-");
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(+10.5, 3)),      "++2105-");
    EXPECT_EQ((elen::encode::large_decimals<'-', '+', float, std::true_type>(+100.5, 4)),     "++31005-");
}

// Chapter 5 Large Decimals
TEST(elen, decode_large_decimals) {
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("--68994+")),   -100.5);
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("--7894+")),    -10.5);
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("-6854+")),     -3.145); // -3854+
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("-685+")),      -3.14);  // -385+
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("-898+")),      -1.01);  // -198+
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("-8+")),        -1);     // -1+
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("-09998766+")), -0.0001233);
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("-0999876+")),  -0.000123);
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("0")),           0);
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("+0000123-")),  +0.000123);
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("+00001233-")), +0.0001233);
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("+1-")),        +1);
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("+101-")),      +1.01);
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("+314-")),      +3.14);
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("+3145-")),     +3.145);
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("++2105-")),    +10.5);
    EXPECT_FLOAT_EQ((elen::decode::large_decimals<float, '-', '+', std::true_type>("++31005-")),   +100.5);
}

// Chapter 5 Large Decimals
TEST(elen, random_large_decimals) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dist(-100, 100);

    std::vector<float> floats;
    std::vector<std::string> strings;
    for(std::size_t i = 0; i < 10; i++) {
        const float value = dist(gen);
        floats.push_back(value);
        strings.push_back(elen::encode::large_decimals(value));
    }

    std::sort(floats.begin(), floats.end());
    std::sort(strings.begin(), strings.end(), lex_comp);

    std::vector<float>::const_iterator f_it = floats.begin();
    std::vector<std::string>::const_iterator s_it = strings.begin();
    while (f_it != floats.end()) {
        EXPECT_FLOAT_EQ(*f_it, (elen::decode::large_decimals<float>(*s_it)));
        f_it++;
        s_it++;
    }
}

// Chapter 6 Floating Pointer Numbers
TEST(elen, encode_floating_point) {
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(-0.1e11, 1)),   "---7888+");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(-0.1e10, 1)),   "---7898+");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(-1.4, 2)),      "--885+");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(-1.3, 2)),      "--886+");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(-1, 1)),        "--88+");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(-0.123, 4)),    "-0876+");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(-0.0123, 4)),   "-+1876+");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(-0.001233, 6)), "-+28766+");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(-0.00123, 5)),  "-+2876+");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(0, 0)),         "0");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(+0.00123, 5)),  "+-7123-");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(+0.001233, 6)), "+-71233-");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(+0.0123, 4)),   "+-8123-");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(+0.123, 3)),    "+0123-");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(+1, 1)),        "++11-");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(+1.3, 2)),      "++113-");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(+1.4, 2)),      "++114-");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(+0.1e10, 1)),   "+++2101-");
    EXPECT_EQ((elen::encode::floating_point<'-', '+', float, std::true_type>(+0.1e11, 1)),   "+++2111-");
}

// Chapter 6 Floating Pointer Numbers
TEST(elen, decode_floating_point) {
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("---7888+")), -0.1e11);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("---7898+")), -0.1e10);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("--885+")),   -1.4);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("--886+")),   -1.3);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("--88+")),    -1);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("-0876+")),   -0.123);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("-+1876+")),  -0.0123);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("-+28766+")), -0.001233);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("-+2876+")),  -0.00123);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("0")),         0);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("+-7123-")),  +0.00123);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("+-71233-")), +0.001233);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("+-8123-")),  +0.0123);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("+0123-")),   +0.123);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("++11-")),    +1);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("++113-")),   +1.3);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("++114-")),   +1.4);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("+++2101-")), +0.1e10);
    EXPECT_FLOAT_EQ((elen::decode::floating_point<float, '-', '+', std::true_type>("+++2111-")), +0.1e11);
}

// Chapter 6 Floating Pointer Numbers
TEST(elen, random_floating_point) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<float> dist(-100, 100);

    std::vector<float> floats;
    std::vector<std::string> strings;
    for(std::size_t i = 0; i < 10; i++) {
        const float value = dist(gen);
        floats.push_back(value);
        strings.push_back(elen::encode::floating_point(value));
    }

    std::sort(floats.begin(), floats.end());
    std::sort(strings.begin(), strings.end(), lex_comp);

    std::vector<float>::const_iterator f_it = floats.begin();
    std::vector<std::string>::const_iterator s_it = strings.begin();
    while (f_it != floats.end()) {
        EXPECT_FLOAT_EQ(*f_it, (elen::decode::floating_point<float>(*s_it)));
        f_it++;
        s_it++;
    }
}
