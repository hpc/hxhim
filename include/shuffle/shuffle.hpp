#ifndef HXHIM_SHUFFLE_BASE
#define HXHIM_SHUFFLE_BASE

#include <functional>

#include "hxhim/Results.hpp"
#include "transport/transport.hpp"

namespace hxhim {

class Shuffle {
    public:
        typedef std::function<int(void *, size_t, void *)> Hash;

        Shuffle(Transport *transport, hxhim::backend::base *backend,
                Hash hash, void *args = nullptr);
        ~Shuffle();

        Results *BPut(void **subjects, std::size_t *subject_lens,
                      void **predicates, std::size_t *predicate_lens,
                      hxhim_type_enum_t *object_types, void **objects, std::size_t *object_lens,
                      const std::size_t count) const;
        Results *BGet(void **subjects, std::size_t *subject_lens,
                      void **predicates, std::size_t *predicate_lens,
                      hxhim_type_enum_t *object_types,
                      const std::size_t count) const;
        Results *BGetOp(void **subjects, std::size_t *subject_lens,
                        void **predicates, std::size_t *predicate_lens,
                        hxhim_type_enum_t *object_types,
                        const std::size_t count) const;
        Results *Delete(void **subjects, std::size_t *subject_lens,
                        void **predicates, std::size_t *predicate_lens,
                        const std::size_t count) const;

    protected:


    private:
        Transport *transport;
        hxhim::backend::base *backend;
        Hash hash;
        void *args;

};

}

#endif
