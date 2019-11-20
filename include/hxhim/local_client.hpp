#ifndef HXHIM_LOCAL_CLIENT_HPP
#define HXHIM_LOCAL_CLIENT_HPP

#include "struct.h"
#include "transport/transport.hpp"

Transport::Response::Put        *local_client_put        (hxhim_t *hx, const Transport::Request::Put         *pm);
Transport::Response::Get        *local_client_get        (hxhim_t *hx, const Transport::Request::Get         *gm);
Transport::Response::Get2       *local_client_get2       (hxhim_t *hx, const Transport::Request::Get2        *gm);
Transport::Response::Delete     *local_client_delete     (hxhim_t *hx, const Transport::Request::Delete      *dm);
Transport::Response::BPut       *local_client_bput       (hxhim_t *hx, const Transport::Request::BPut        *bpm);
Transport::Response::BGet       *local_client_bget       (hxhim_t *hx, const Transport::Request::BGet        *bgm);
Transport::Response::BGet2      *local_client_bget2      (hxhim_t *hx, const Transport::Request::BGet2       *bgm);
Transport::Response::BGetOp     *local_client_bget_op    (hxhim_t *hx, const Transport::Request::BGetOp      *gm);
Transport::Response::BDelete    *local_client_bdelete    (hxhim_t *hx, const Transport::Request::BDelete     *bdm);

Transport::Response::Histogram  *local_client_histogram  (hxhim_t *hx, const Transport::Request::Histogram   *hist);
Transport::Response::BHistogram *local_client_bhistogram (hxhim_t *hx, const Transport::Request::BHistogram  *bhist);

#endif
