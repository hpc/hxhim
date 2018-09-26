#ifndef HXHIM_MAX_SIZE_HPP
#define HXHIM_MAX_SIZE_HPP

#include <cstddef>

namespace hxhim {
namespace MaxSize {

/**
 * These functions return the size of the largest data structure
 */
std::size_t Bulks();
std::size_t Arrays();
std::size_t Requests();
std::size_t Responses();
std::size_t Result();

}
}

#endif
