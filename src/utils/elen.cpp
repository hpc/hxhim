namespace elen {

// negatives get their digits flipped (9 - digit)
void flip(std::string &str) {
    for(char &c : str) {
        if (std::isdigit(c)) {
            c = '9' - c + '0';
        }
    }
}

namespace encode {

// Chapter 2.3 Uniqueness
template <typename T, typename = std::enable_if<std::is_integral<T>::value> >
std::string encode(const T value, const char prefix, const char pos, const char neg) {
    if (value == 0) {
        return "0";
    }

    std::string out;
    if (value > 0) {
        out += prefix;
    }

    std::string str = std::to_string(value);
    if (str.size() > 1) {
        out += encode<T>(str.size(), prefix, pos, neg);
    }

    // negatives get their digits flipped
    if (prefix == neg) {
        flip(str);
    }

    return out + str;
}

// Chapter 3 Integers
template <typename T, typename IsIntegral>
std::string integers(const T value, const char pos, const char neg) {
    if (value < 0) {
        return encode<T, IsIntegral>(-value, neg, pos, neg);
    }
    else if (value > 0) {
        return encode<T, IsIntegral>(value, pos, pos, neg);
    }

    return "0";
}

template <typename T, typename IsFloatingPoint>
std::string small_decimals_digits(const T value, const int precision, const char pos, const char neg) {
    if (value == 0) {
        return "0";
    }

    std::stringstream s;
    s << std::setprecision(precision) << std::abs(value);

    // remove leading zero and decimal point
    char tmp[2];
    s.read(tmp, 2);

    // read out digits
    std::string out;
    s >> out;

    // negatives get their digits flipped
    if (value < 0) {
        flip(out);
    }

    return out;
}

// Chapter 4 Small Decimals
// value is in (0, 1)
template <typename T, typename IsFloatingPoint>
std::string small_decimals(const T value, const int precision, const char pos, const char neg) {
    std::string out = small_decimals_digits<T, IsFloatingPoint>(value, precision, pos, neg);

    if (value < 0) {
        out = neg + out + pos;
    }
    else if (value > 0) {
        out = pos + out + neg;
    }

    return out;
}

template <typename T, typename IsFloatingPoint>
std::string large_decimals(const T value, const int precision, const char pos, const char neg) {
    if (value == 0) {
        return "0";
    }

    std::stringstream s;
    s << std::setprecision(precision) << value;

    int integer = 0;
    T decimal = 0;
    s >> integer >> decimal;

    if (value < 0) {
        decimal = -decimal;
    }

    std::string out;
    if (integer != 0) {
        out = integers(integer, pos, neg);
    }
    else {
        if (value < 0) {
            out = "-0";
        }
        else {
            out = "+0";
        }
    }
    if (decimal != 0){
        out += small_decimals_digits<T, IsFloatingPoint>(decimal, precision, pos, neg);
    }

    if (value < 0) {
        out += pos;
    }
    else {
        out += neg;
    }

    return out;
}

// https://stackoverflow.com/a/29583280
template <typename R, typename Z, typename = std::enable_if<std::is_arithmetic<R>::value &&
                                                            std::is_integral<Z>::value> >
R frexp10(const R arg, Z *exp) {
    *exp = (arg == 0) ? 0 : (Z)(1 + std::round(std::log10(std::fabs(arg))));
    return arg * pow(10 , -(*exp));
}

// Chapter 6 Floating Pointer Numbers
template <typename T, typename IsFloatingPoint>
std::string floating_point(const T value, const int precision, const char pos, const char neg) {
    if (value == 0) {
        return "0";
    }

    int exp = 0;
    T mantissa = frexp10(value, &exp);

    std::string out;
    if (value < 0) {
        out += neg;
        exp = -exp;
    }
    else if (value > 0) {
        out += pos;
    }

    if (exp != 0) {
        out += integers(exp, pos, neg);
    }
    else {
        out += "0";
    }

    if (mantissa != 0) {
        out += small_decimals_digits<T, IsFloatingPoint>(mantissa, precision, pos, neg);
    }

    if (value < 0) {
        out += pos;
    }
    else if (value > 0) {
        out += neg;
    }

    return out;
}

}

namespace decode {

std::size_t get_prefix_count(const char prefix, const std::string &str, const char pos, const char neg) {
    if (str[0] == '0') {
        return 0;
    }

    // count the number of prefix symbols
    if ((prefix != pos) && (prefix != neg)) {
        throw std::runtime_error(std::string("Bad prefix: ") + prefix);
    }

    std::size_t prefix_count = 0;
    while ((prefix_count < str.size()) && (str[prefix_count] == prefix)) {
        prefix_count++;
    }

    // end of string
    if (prefix_count == str.size()) {
        throw std::runtime_error("Bad input: " + str);
    }

    // bad symbol
    if (!std::isdigit(str[prefix_count])) {
        throw std::runtime_error("Bad input: " + str);
    }

    return prefix_count;
}

template <typename T, typename = std::enable_if<std::is_integral<T>::value> >
T next(const char prefix, const std::string &str, std::size_t &position, const T &len, const char pos, const char neg) {
    // read the next series of digits
    std::string str_len = str.substr(position, len);

    // if the value was negative, flip the digits
    if (prefix == neg) {
        flip(str_len);
    }

    // move the position by the original length
    position += len;

    // the digits become the new length
    std::stringstream s(str_len);
    T out = 0;

    if (!(s >> out)) {
        throw std::runtime_error("Bad next set of digits: " + str_len);
    }

    return out;
}

// Chapter 3 Integers
template <typename T, typename IsIntegral>
T integers(const std::string &str, const char pos, const char neg) {
    if (str.size() == 0) {
        throw std::runtime_error("Empty string");
    }

    if (str.size() == 1) {
        if (str == "0") {
            return 0;
        }
        else {
            throw std::runtime_error("Bad input: " + str);
        }
    }

    const char prefix = str[0];
    const std::size_t prefix_count = get_prefix_count(prefix, str, pos, neg);

    // read the encoded lengths
    std::size_t position = prefix_count;
    std::size_t len = 1;
    for(std::size_t i = 0; i < prefix_count - 1; i++) {
        len = next(prefix, str, position, len, pos, neg);
    }

    // read the final value
    T out = next(prefix, str, position, len, pos, neg);
    if (prefix == neg) {
        out = -out;
    }

    return out;
}

// Chapter 4 Small Decimals
// value is in (0, 1)
template <typename T, typename IsFloatingPoint>
T small_decimals(const std::string &str, const char pos, const char neg) {
    if (str.size() == 0) {
        throw std::runtime_error("Empty string");
    }

    if (str.size() == 1) {
        if (str == "0") {
            return 0;
        }
        else {
            throw std::runtime_error("Bad input: " + str);
        }
    }

    if (str.size() == 2) {
        throw std::runtime_error("Bad input: " + str);
    }

    std::string digits = str.substr(1, str.size() - 2);
    std::stringstream s;
    if ((str.front() == neg) && (str.back() == pos)) {
        s << "-";
        flip(digits);
    }
    else if ((str.front() == pos) && (str.back() == neg)) {}
    else {
        throw std::runtime_error("Mismatched prefix and postfix: " + str);
    }
    s << "0." << digits;

    T out;
    s >> out;
    return out;
}

// Chapter 5 Large Decimals
template <typename T, typename IsFloatingPoint>
T large_decimals(const std::string &str, const char pos, const char neg) {
    if (str.size() == 0) {
        throw std::runtime_error("Empty string");
    }

    if (str.size() == 1) {
        if (str == "0") {
            return 0;
        }
        else {
            throw std::runtime_error("Bad input: " + str);
        }
    }

    if (str.size() == 2) {
        throw std::runtime_error("Bad input: " + str);
    }

    int integer = 0;
    const char prefix = str[0];
    const std::size_t prefix_count = get_prefix_count(prefix, str, pos, neg);
    std::size_t position = prefix_count;

    // only decode integer portion if the integer is not 0
    if (str[position] != '0') {
        // read the encoded lengths
        position = prefix_count;
        std::size_t len = 1;
        for(std::size_t i = 0; i < prefix_count - 1; i++) {
            len = next(prefix, str, position, len, pos, neg);
        }

        // the final section is the integral value
        integer = next(prefix, str, position, len, pos, neg);
        if (prefix == neg) {
            integer = -integer;
        }
    }
    else {
        position++;
    }

    T out = integer;

    // read the decimal part, if there is one
    if (position != str.size() - 1) {
        out += small_decimals<T>(prefix + str.substr(position, str.size() - position), pos, neg);
    }

    return out;
}

// Chapter 6 Floating Pointer Numbers
template <typename T, typename IsFloatingPoint>
T floating_point(const std::string &str, const char pos, const char neg) {
    if (str.size() == 0) {
        throw std::runtime_error("Empty string");
    }

    if (str.size() == 1) {
        if (str == "0") {
            return 0;
        }
        else {
            throw std::runtime_error("Bad input: " + str);
        }
    }

    if (str.size() == 2) {
        throw std::runtime_error("Bad input: " + str);
    }

    // get the sign of the mantissa
    char sign = 0;
    if ((str.front() == neg) && (str.back() == pos)) {
        sign = neg;
    }
    else if ((str.front() == pos) && (str.back() == neg)) {
        sign = pos;
    }
    else {
        throw std::runtime_error("Mismatched prefix and postfix: " + str);
    }

    int exp = 0;
    const std::string wo_sign = str.substr(1, str.size() - 1);
    const char prefix = wo_sign[0];
    const std::size_t prefix_count = get_prefix_count(prefix, wo_sign, pos, neg);
    std::size_t position = prefix_count;

    // only decode exponent portion if the exponent is not 0
    if (wo_sign[position] != '0') {
        // read the encoded lengths
        position = prefix_count;
        std::size_t len = 1;
        for(std::size_t i = 0; i < prefix_count - 1; i++) {
            len = next(prefix, wo_sign, position, len, pos, neg);
        }

        // the final section is the integral exponent value
        exp = next(prefix, wo_sign, position, len, pos, neg);
        if (prefix == neg) {
            exp = -exp;
        }
    }
    else {
        position++;
    }

    T mantissa = 1;

    // read the decimal part, if there is one
    if (position != str.size() - 1) {
        mantissa = small_decimals<T>(sign + wo_sign.substr(position, wo_sign.size() - position), pos, neg);
    }

    return std::pow(10, ((sign == neg)?-1:1) * exp) * mantissa;
}

}
}
