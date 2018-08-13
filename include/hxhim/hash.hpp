#ifndef HXHIM_HASH_HPP
#define HXHIM_HASH_HPP

#include <functional>

#include "hxhim/struct.h"

namespace hxhim {
class hash {
    public:
        static void init(hxhim_t *hx);
        static void destroy();

        typedef std::function<int(void *, const std::size_t, void *, const std::size_t, void *)> Func;

        static int Rank(void *subject, const std::size_t subject_len, void *predicate, const std::size_t predicate_len, void *args);
        static int SumModDatastores(void *subject, const std::size_t subject_len, void *predicate, const std::size_t predicate_len, void *args);

    private:
        static hxhim_t *hx_;
};

}

#endif
