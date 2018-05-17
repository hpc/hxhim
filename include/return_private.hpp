#ifndef HXHIM_RETURN_PRIVATE_HPP
#define HXHIM_RETURN_PRIVATE_HPP

#include "return.hpp"

#ifdef __cplusplus
extern "C"
{
#endif

hxhim_return_t *hxhim_return_init(hxhim::Return *ret);
hxhim_get_return_t *hxhim_get_return_init(hxhim::GetReturn *ret);

#ifdef __cplusplus
}
#endif

#endif
