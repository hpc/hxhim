#include "transport/Options.hpp"

/**
 * hxhim_options_set_transport
 * Sets the values needed to set up the Transport
 * This function moves ownership of the config function from the caller to opts
 *
 * @param opts   the set of options to be modified
 * @param config configuration data needed to initialize the Transport
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_transport(hxhim_options_t *opts, Transport::Options *config) {
    if (!valid_opts(opts)) {
        return HXHIM_ERROR;
    }

    delete opts->p->transport;

    opts->p->transport = config;

    return HXHIM_SUCCESS;
}

/**
 * hxhim_options_set_transport_null
 * Sets the values needed to set up a null Transport
 *
 * @param opts              the set of options to be modified
 * @param listeners         the number of listeners
 * @return HXHIM_SUCCESS or HXHIM_ERROR
 */
int hxhim_options_set_transport_null(hxhim_options_t *opts) {
    if (!opts || !opts->p) {
        return HXHIM_ERROR;
    }

    Transport::Options *config = new Transport::Options(Transport::TRANSPORT_NULL);
    if (!config) {
        return HXHIM_ERROR;
    }

    if (hxhim_options_set_transport(opts, config) != HXHIM_SUCCESS) {
        delete config;
        return HXHIM_ERROR;
    }

    return HXHIM_SUCCESS;
}
