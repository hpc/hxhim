#include <iomanip>
#include <iostream>

#include "utils/elen.hpp"

const std::string ENCODE = "encode";
const std::string DECODE = "decode";

const std::string INT    = "int";
const std::string FLOAT  = "float";
const std::string DOUBLE = "double";

int main(int argc, char *argv[]) {
    char neg = elen::NEG_SYMBOL;
    char pos = elen::POS_SYMBOL;
    std::size_t precision = 0;
    std::size_t *pre = nullptr;

    for(int i = 1; i < argc; i++) {
        const std::string args = argv[i];
        if ((args == "-h") || (args == "--help")) {
            std::cout << "Syntax: " << argv[0] << " [--neg c] [--pos c] [--digits d]" << std::endl
                      << std::endl
                      << "    Input is read from stdin" << std::endl
                      << "    Input format is: <encode|decode> <int|float|double> <value>" << std::endl
                      << "    Value should be numeric when encoding and a string when decoding." << std::endl;
            return 0;
        }
        else if (args == "--neg") {
            neg = argv[++i][0];
        }
        else if (args == "--pos") {
            pos = argv[++i][0];
        }
        else if (args == "--digits") {
            if (sscanf(argv[++i], "%zu", &precision) != 1) {
                std::cerr << "Bad Precision: " << args << std::endl;
                return 1;
            }

            pre = &precision;
        }

        i++;
    }

    std::string op, type;
    while (std::cin >> op >> type) {
        if (op == ENCODE) {
            if (type == INT) {
                int64_t value;
                if (!(std::cin >> value)) {
                    std::cerr << "Error Bad float value" << std::endl;
                    continue;
                }

                std::cout << elen::encode::integers(value, neg, pos) << std::endl;

            }
            else if (type == FLOAT) {
                float value;
                if (!(std::cin >> value)) {
                    std::cerr << "Error Bad float value" << std::endl;
                    continue;
                }

                std::cout << elen::encode::floating_point(value,
                                                          pre?*pre:std::numeric_limits<float>::digits10,
                                                          neg, pos) << std::endl;
            }
            else if (type == DOUBLE) {
                double value;
                if (!(std::cin >> value)) {
                    std::cerr << "Error Bad double value" << std::endl;
                    continue;
                }

                std::cout << elen::encode::floating_point(value,
                                                          pre?*pre:std::numeric_limits<double>::digits10,
                                                          neg, pos) << std::endl;
            }
            else {
                std::cerr << "Unknown numeric type: " << type << std::endl;
            }
        }
        else if (op == DECODE) {
            std::string value;
            if (!(std::cin >> value)) {
                std::cerr << "Error Bad string" << std::endl;
                continue;
            }

            if (type == INT) {
                std::cout << elen::decode::integers<int64_t>(value, nullptr, neg, pos) << std::endl;
            }
            else if (type == FLOAT) {
                std::cout << std::fixed
                          << std::setprecision(pre?*pre:(std::numeric_limits<float>::digits10) + 1)
                          << elen::decode::floating_point<float>(value, neg, pos) << std::endl;
            }
            else if (type == DOUBLE) {
                std::cout << std::fixed
                          << std::setprecision(pre?*pre:(std::numeric_limits<float>::digits10) + 1)
                          << elen::decode::floating_point<double>(value, neg, pos) << std::endl;
            }
            else {
                std::cerr << "Unknown numeric type: " << type << std::endl;
            }
        }
        else {
               std::cerr << "Unknown operation: " << op << std::endl;
         }
    }

    return 0;
}
