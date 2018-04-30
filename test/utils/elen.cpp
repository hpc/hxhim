#include <sstream>

#include <gtest/gtest.h>

#include "elen.hpp"

// Efficient Lexicographic Encoding of Numbers
// Peter Seymour
// https://github.com/jordanorelli/lexnum

// Chapter 3 Integers
TEST(elen, encode_integers) {
    EXPECT_EQ(elen::encode::integers(-1234567891), "---7898765432108");
    EXPECT_EQ(elen::encode::integers(-1234567890), "---7898765432109");
    EXPECT_EQ(elen::encode::integers(-1234567889), "---7898765432110");

    EXPECT_EQ(elen::encode::integers(-11), "--788");
    EXPECT_EQ(elen::encode::integers(-10), "--789");
    EXPECT_EQ(elen::encode::integers(-9), "-0");

    EXPECT_EQ(elen::encode::integers(-2), "-7");
    EXPECT_EQ(elen::encode::integers(-1), "-8");
    EXPECT_EQ(elen::encode::integers(0), "0");
    EXPECT_EQ(elen::encode::integers(+1), "+1");
    EXPECT_EQ(elen::encode::integers(+2), "+2");

    EXPECT_EQ(elen::encode::integers(+9), "+9");
    EXPECT_EQ(elen::encode::integers(+10), "++210");
    EXPECT_EQ(elen::encode::integers(+11), "++211");

    EXPECT_EQ(elen::encode::integers(+1234567889), "+++2101234567889");
    EXPECT_EQ(elen::encode::integers(+1234567890), "+++2101234567890");
    EXPECT_EQ(elen::encode::integers(+1234567891), "+++2101234567891");
}

// Chapter 3 Integers
TEST(elen, decode_integers) {
    EXPECT_EQ(elen::decode::integers<int32_t>("---7898765432108"), -1234567891);
    EXPECT_EQ(elen::decode::integers<int32_t>("---7898765432109"), -1234567890);
    EXPECT_EQ(elen::decode::integers<int32_t>("---7898765432110"), -1234567889);

    EXPECT_EQ(elen::decode::integers<int32_t>("--788"), -11);
    EXPECT_EQ(elen::decode::integers<int32_t>("--789"), -10);
    EXPECT_EQ(elen::decode::integers<int32_t>("-0"), -9);

    EXPECT_EQ(elen::decode::integers<int32_t>("-7"), -2);
    EXPECT_EQ(elen::decode::integers<int32_t>("-8"), -1 );
    EXPECT_EQ(elen::decode::integers<int32_t>("0"), 0);
    EXPECT_EQ(elen::decode::integers<int32_t>("+1"), 1);
    EXPECT_EQ(elen::decode::integers<int32_t>("+2"), 2);

    EXPECT_EQ(elen::decode::integers<int32_t>("+9"), 9);
    EXPECT_EQ(elen::decode::integers<int32_t>("++210"), 10);
    EXPECT_EQ(elen::decode::integers<int32_t>("++211"), 11);

    EXPECT_EQ(elen::decode::integers<int32_t>("+++2101234567889"), +1234567889);
    EXPECT_EQ(elen::decode::integers<int32_t>("+++2101234567890"), +1234567890);
    EXPECT_EQ(elen::decode::integers<int32_t>("+++2101234567891"), +1234567891);
}

// Chapter 4 Small Decimals
TEST(elen, encode_small_decimals) {
    EXPECT_EQ(elen::encode::small_decimals(-0.9995, 4), "-0004+");
    EXPECT_EQ(elen::encode::small_decimals(-0.999, 3), "-000+");
    EXPECT_EQ(elen::encode::small_decimals(-0.0123, 4), "-9876+");
    EXPECT_EQ(elen::encode::small_decimals(-0.00123, 5), "-99876+");
    EXPECT_EQ(elen::encode::small_decimals(-0.0001233, 7), "-9998766+");
    EXPECT_EQ(elen::encode::small_decimals(-0.000123, 6), "-999876+");
    EXPECT_EQ(elen::encode::small_decimals(0, 0), "0");
    EXPECT_EQ(elen::encode::small_decimals(+0.000123, 6), "+000123-");
    EXPECT_EQ(elen::encode::small_decimals(+0.0001233, 7), "+0001233-");
    EXPECT_EQ(elen::encode::small_decimals(+0.00123, 5), "+00123-");
    EXPECT_EQ(elen::encode::small_decimals(+0.0123, 4), "+0123-");
    EXPECT_EQ(elen::encode::small_decimals(+0.999, 3), "+999-");
    EXPECT_EQ(elen::encode::small_decimals(+0.9995, 4), "+9995-");
}

// Chapter 4 Small Decimals
TEST(elen, decode_small_decimals) {
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("-0004+"),    -0.9995);
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("-000+"),     -0.999);
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("-9876+"),    -0.0123);
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("-99876+"),   -0.00123);
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("-9998766+"), -0.0001233);
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("-999876+"),  -0.000123);
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("0"),          0);
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("+000123-"),  +0.000123);
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("+0001233-"), +0.0001233);
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("+00123-"),   +0.00123);
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("+0123-"),    +0.0123);
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("+999-"),     +0.999);
    EXPECT_FLOAT_EQ(elen::decode::small_decimals<float>("+9995-"),    +0.9995);
}

// Chapter 5 Large Decimals
TEST(elen, encode_large_decimals) {
    EXPECT_EQ(elen::encode::large_decimals(-100.5, 4),     "--68994+");
    EXPECT_EQ(elen::encode::large_decimals(-10.5, 3),      "--7894+");
    EXPECT_EQ(elen::encode::large_decimals(-3.145, 4),     "-6854+"); // -3854+
    EXPECT_EQ(elen::encode::large_decimals(-3.14, 3),      "-685+");  // -385+
    EXPECT_EQ(elen::encode::large_decimals(-1.01, 3),      "-898+");  // -198+
    EXPECT_EQ(elen::encode::large_decimals(-1, 1),         "-8+");    // -1+
    EXPECT_EQ(elen::encode::large_decimals(-0.0001233, 7), "-09998766+");
    EXPECT_EQ(elen::encode::large_decimals(-0.000123, 6),  "-0999876+");
    EXPECT_EQ(elen::encode::large_decimals(0, 0),          "0");
    EXPECT_EQ(elen::encode::large_decimals(+0.000123, 6),  "+0000123-");
    EXPECT_EQ(elen::encode::large_decimals(+0.0001233, 7), "+00001233-");
    EXPECT_EQ(elen::encode::large_decimals(+1, 1),         "+1-");
    EXPECT_EQ(elen::encode::large_decimals(+1.01, 3),      "+101-");
    EXPECT_EQ(elen::encode::large_decimals(+3.14, 3),      "+314-");
    EXPECT_EQ(elen::encode::large_decimals(+3.145, 4),     "+3145-");
    EXPECT_EQ(elen::encode::large_decimals(+10.5, 3),      "++2105-");
    EXPECT_EQ(elen::encode::large_decimals(+100.5, 4),     "++31005-");
}

// Chapter 5 Large Decimals
TEST(elen, decode_large_decimals) {
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("--68994+"),   -100.5);
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("--7894+"),    -10.5);
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("-6854+"),     -3.145); // -3854+
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("-685+"),      -3.14);  // -385+
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("-898+"),      -1.01);  // -198+
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("-8+"),        -1);     // -1+
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("-09998766+"), -0.0001233);
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("-0999876+"),  -0.000123);
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("0"),           0);
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("+0000123-"),  +0.000123);
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("+00001233-"), +0.0001233);
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("+1-"),        +1);
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("+101-"),      +1.01);
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("+314-"),      +3.14);
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("+3145-"),     +3.145);
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("++2105-"),    +10.5);
    EXPECT_FLOAT_EQ(elen::decode::large_decimals<float>("++31005-"),   +100.5);
}

// Chapter 6 Floating Pointer Numbers
TEST(elen, encode_floating_point) {
    EXPECT_EQ(elen::encode::floating_point((float) -0.1e11, 1),   "---7888+");
    EXPECT_EQ(elen::encode::floating_point((float) -0.1e10, 1),   "---7898+");
    EXPECT_EQ(elen::encode::floating_point((float) -1.4, 2),      "--885+");
    EXPECT_EQ(elen::encode::floating_point((float) -1.3, 2),      "--886+");
    EXPECT_EQ(elen::encode::floating_point((float) -1, 1),        "--88+");
    EXPECT_EQ(elen::encode::floating_point((float) -0.123, 4),    "-0876+");
    EXPECT_EQ(elen::encode::floating_point((float) -0.0123, 4),   "-+1876+");
    EXPECT_EQ(elen::encode::floating_point((float) -0.001233, 6), "-+28766+");
    EXPECT_EQ(elen::encode::floating_point((float) -0.00123, 5),  "-+2876+");
    EXPECT_EQ(elen::encode::floating_point((float) 0, 0),         "0");
    EXPECT_EQ(elen::encode::floating_point((float) +0.00123, 5),  "+-7123-");
    EXPECT_EQ(elen::encode::floating_point((float) +0.001233, 6), "+-71233-");
    EXPECT_EQ(elen::encode::floating_point((float) +0.0123, 4),   "+-8123-");
    EXPECT_EQ(elen::encode::floating_point((float) +0.123, 3),    "+0123-");
    EXPECT_EQ(elen::encode::floating_point((float) +1, 1),        "++11-");
    EXPECT_EQ(elen::encode::floating_point((float) +1.3, 2),      "++113-");
    EXPECT_EQ(elen::encode::floating_point((float) +1.4, 2),      "++114-");
    EXPECT_EQ(elen::encode::floating_point((float) +0.1e10, 1),   "+++2101-");
    EXPECT_EQ(elen::encode::floating_point((float) +0.1e11, 1),   "+++2111-");
}

// Chapter 6 Floating Pointer Numbers
TEST(elen, decode_floating_point) {
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("---7888+"), -0.1e11);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("---7898+"), -0.1e10);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("--885+"),   -1.4);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("--886+"),   -1.3);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("--88+"),    -1);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("-0876+"),   -0.123);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("-+1876+"),  -0.0123);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("-+28766+"), -0.001233);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("-+2876+"),  -0.00123);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("0"),         0);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("+-7123-"),  +0.00123);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("+-71233-"), +0.001233);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("+-8123-"),  +0.0123);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("+0123-"),   +0.123);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("++11-"),    +1);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("++113-"),   +1.3);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("++114-"),   +1.4);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("+++2101-"), +0.1e10);
    EXPECT_FLOAT_EQ(elen::decode::floating_point<float>("+++2111-"), +0.1e11);
}
