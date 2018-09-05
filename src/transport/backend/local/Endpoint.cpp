#include "transport/backend/local/Endpoint.hpp"
#include "hxhim/local_client.hpp"

namespace Transport {
namespace local {

Endpoint::Endpoint(hxhim_t *hx)
  : ::Transport::Endpoint(),
    hx(hx)
{}

/**
 * Put
 * Sends a Request::Put to the other end of the endpoint
 *
 * @param request the initiating PUT message
 * @return a pointer to the response of the PUT operation
 */
Response::Put *Endpoint::Put(const Request::Put *message) {
    return local_client_put(hx, message);
}

/**
 * Get
 * Sends a Request::Get to the other end of the endpoint
 *
 * @param request the initiating GET message
 * @return a pointer to the response of the GET operation
 */
Response::Get *Endpoint::Get(const Request::Get *message) {
    return local_client_get(hx, message);
}

/**
 * Delete
 * Sends a Request::Delete to the other end of the endpoint
 *
 * @param request the initiating DELETE message
 * @return a pointer to the response of the DELETE operation
 */
Response::Delete *Endpoint::Delete(const Request::Delete *message) {
    return local_client_delete(hx, message);
}

/**
 * Histogram
 * Sends a Request::Histogram to the other end of the endpoint
 *
 * @param request the initiating HISTOGRAM message
 * @return a pointer to the response of the HISTOGRAM operation
 */
Response::Histogram *Endpoint::Histogram(const Request::Histogram *message) {
    return local_client_histogram(hx, message);
}

}
}
