#include "datastore/datastore.hpp"

static const char *subs[]                = {"sub0",  "sub1"};
static const char *preds[]               = {"pred0", "pred1"};
static const hxhim_object_type_t types[] = {hxhim_object_type_t::HXHIM_OBJECT_TYPE_BYTE,
                                            hxhim_object_type_t::HXHIM_OBJECT_TYPE_BYTE};
static const char *objs[]                = {"obj0",   "obj1"};
static const std::size_t count           = sizeof(types) / sizeof(types[0]);

/*
  If all 6 combinations are enabled:

  obj0  pred0 sub0
  obj0  sub0  pred0
  obj1  pred1 sub1
  obj1  sub1  pred1
  pred0 obj0  sub0
  pred0 sub0  obj0
  pred1 obj1  sub1
  pred1 sub1  obj1
  sub0  obj0  pred0
  sub0  pred0 obj0
  sub1  obj1  pred1
  sub1  pred1 obj1
 */
static const std::size_t total = count * HXHIM_PUT_MULTIPLIER;
