#ifndef TRANSPORT_LOCAL_ENDPOINT_GROUP_HPP
#define TRANSPORT_LOCAL_ENDPOINT_GROUP_HPP

#include <map>

#include "hxhim/struct.h"
#include "transport/transport.hpp"
#include "utils/FixedBufferPool.hpp"

namespace Transport {
namespace local {

/**
 * MPIEndpointGroup
 * Collective communication endpoint implemented with MPI
 */
class EndpointGroup : virtual public ::Transport::EndpointGroup {
    public:
        EndpointGroup(hxhim_t *hx);

        ~EndpointGroup();

        /** @description Bulk Put to multiple endpoints    */
        Response::BPut *BPut(const std::size_t num_rangesrvs, Request::BPut **bpm_list);

        /** @description Bulk Get from multiple endpoints  */
        Response::BGet *BGet(const std::size_t num_rangesrvs, Request::BGet **bgm_list);

        /** @description Bulk Get from multiple endpoints  */
        Response::BGetOp *BGetOp(const std::size_t num_rangesrvs, Request::BGetOp **bgm_list);

        /** @description Bulk Delete to multiple endpoints */
        Response::BDelete *BDelete(const std::size_t num_rangesrvs, Request::BDelete **bdm_list);

        /** @description Bulk Histogram to multiple endpoints */
        Response::BHistogram *BHistogram(const std::size_t num_rangesrvs, Request::BHistogram **bhist_list);

    private:
        hxhim_t *hx;
};

}
}

#endif
