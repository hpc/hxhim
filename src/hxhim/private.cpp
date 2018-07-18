#include <cfloat>

#include "hxhim/private.hpp"

namespace hxhim {

}

hxhim_private::hxhim_private()
    : subject_type(HXHIM_SPO_BYTE_TYPE),
      predicate_type(HXHIM_SPO_BYTE_TYPE),
      object_type(HXHIM_SPO_BYTE_TYPE),
      backend(nullptr),
      running(false),
      puts(),
      gets(),
      getops(),
      deletes(),
      queued_bputs(0),
      background_put_thread(),
      put_results_mutex(),
      put_results(nullptr),
      histogram(nullptr)
{}
