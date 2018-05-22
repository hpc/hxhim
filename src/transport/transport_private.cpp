#include "transport_private.hpp"

mdhim_rm_t *mdhim_rm_init(TransportRecvMessage *rm) {
    if (!rm) {
        return nullptr;
    }

    mdhim_rm_t *ret = new mdhim_rm_t();
    if (!ret) {
        return nullptr;
    }

    ret->rm = rm;
    return ret;
}

mdhim_brm_t *mdhim_brm_init(TransportBRecvMessage *brm) {
    if (!brm) {
        return nullptr;
    }

    mdhim_brm_t *ret = new mdhim_brm_t();
    if (!ret) {
        return nullptr;
    }

    ret->brm = brm;
    return ret;
}

mdhim_grm_t *mdhim_grm_init(TransportGetRecvMessage *grm) {
    if (!grm) {
        return nullptr;
    }

    mdhim_grm_t *ret = new mdhim_grm_t();
    if (!ret) {
        return nullptr;
    }

    ret->grm = grm;
    return ret;
}

mdhim_bgrm_t *mdhim_bgrm_init(TransportBGetRecvMessage *bgrm) {
    if (!bgrm) {
        return nullptr;
    }

    mdhim_bgrm_t *ret = new mdhim_bgrm_t();
    if (!ret) {
        return nullptr;
    }

    ret->bgrm = bgrm;
    return ret;
}

void mdhim_rm_destroy(mdhim_rm_t *rm) {
    if (rm) {
        delete rm->rm;
    }

    delete rm;
}

void mdhim_brm_destroy(mdhim_brm_t *brm) {
    if (brm) {
        delete brm->brm;
    }

    delete brm;
}

void mdhim_grm_destroy(mdhim_grm_t *grm) {
    if (grm) {
        delete grm->grm;
    }

    delete grm;
}

void mdhim_bgrm_destroy(mdhim_bgrm_t *bgrm) {
    if (bgrm) {
        delete bgrm->bgrm;
    }

    delete bgrm;
}

int mdhim_rm_src(const mdhim_rm_t *rm, int *src) {
    if (!rm || !rm->rm || !src) {
        return MDHIM_ERROR;
    }

    *src = rm->rm->src;
    return MDHIM_SUCCESS;
}

int mdhim_rm_rs_idx(const mdhim_rm_t *rm, int *rs_idx) {
    if (!rm || !rm->rm || !rs_idx) {
        return MDHIM_ERROR;
    }

    *rs_idx = rm->rm->rs_idx;
    return MDHIM_SUCCESS;
}

int mdhim_rm_error(const mdhim_rm_t *rm, int *error) {
    if (!rm || !rm->rm || !error) {
        return MDHIM_ERROR;
    }

    *error = rm->rm->error;
    return MDHIM_SUCCESS;
}

int mdhim_brm_src(const mdhim_brm_t *brm, int *src) {
    if (!brm || !brm->brm || !src) {
        return MDHIM_ERROR;
    }

    *src = brm->brm->src;
    return MDHIM_SUCCESS;
}

int mdhim_brm_rs_idx(const mdhim_brm_t *brm, int **rs_idx) {
    if (!brm || !brm->brm || !rs_idx) {
        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < brm->brm->num_keys; i++) {
        (*rs_idx)[i] = brm->brm->rs_idx[i];
    }

    return MDHIM_SUCCESS;
}

int mdhim_brm_error(const mdhim_brm_t *brm, int *error) {
    if (!brm || !brm->brm || !error) {
        return MDHIM_ERROR;
    }

    *error = brm->brm->error;
    return MDHIM_SUCCESS;
}

int mdhim_brm_num_keys(const mdhim_brm_t *brm, size_t *num_keys) {
    if (!brm || !brm->brm || !num_keys) {
        return MDHIM_ERROR;
    }

    *num_keys = brm->brm->num_keys;
    return MDHIM_SUCCESS;
}

int mdhim_brm_next(const mdhim_brm_t *brm, mdhim_brm_t **next) {
    if (!brm || !brm->brm || !next) {
        return MDHIM_ERROR;
    }

    *next = nullptr;

    if (brm->brm->next) {
        *next = mdhim_brm_init(brm->brm->next);
    }

    return MDHIM_SUCCESS;
}

int mdhim_grm_src(const mdhim_grm_t *grm, int *src) {
    if (!grm || !grm->grm || !src) {
        return MDHIM_ERROR;
    }

    *src = grm->grm->src;
    return MDHIM_SUCCESS;
}

int mdhim_grm_rs_idx(const mdhim_grm_t *grm, int *rs_idx) {
    if (!grm || !grm->grm || !rs_idx) {
        return MDHIM_ERROR;
    }

    *rs_idx = grm->grm->rs_idx;
    return MDHIM_SUCCESS;
}

int mdhim_grm_error(const mdhim_grm_t *grm, int *error) {
    if (!grm || !grm->grm || !error) {
        return MDHIM_ERROR;
    }

    *error = grm->grm->error;
    return MDHIM_SUCCESS;
}

int mdhim_grm_key(const mdhim_grm_t *grm, void **key, std::size_t *key_len) {
    if (!grm || !grm->grm) {
        return MDHIM_ERROR;
    }

    if (key) {
        *key = grm->grm->key;
    }

    if (key_len) {
        *key_len = grm->grm->key_len;
    }

    return MDHIM_SUCCESS;
}

int mdhim_grm_value(const mdhim_grm_t *grm, void **value, std::size_t *value_len) {
    if (!grm || !grm->grm) {
        return MDHIM_ERROR;
    }

    if (value) {
        *value = grm->grm->value;
    }

    if (value_len) {
        *value_len = grm->grm->value_len;
    }

    return MDHIM_SUCCESS;
}

int mdhim_bgrm_src(const mdhim_bgrm_t *bgrm, int *src) {
    if (!bgrm || !bgrm->bgrm || !src) {
        return MDHIM_ERROR;
    }

    *src = bgrm->bgrm->src;
    return MDHIM_SUCCESS;
}

int mdhim_bgrm_rs_idx(const mdhim_bgrm_t *bgrm, int **rs_idx) {
    if (!bgrm || !bgrm->bgrm || !rs_idx || !*rs_idx) {
        return MDHIM_ERROR;
    }

    for(std::size_t i = 0; i < bgrm->bgrm->num_keys; i++) {
        (*rs_idx)[i] = bgrm->bgrm->rs_idx[i];
    }

    return MDHIM_SUCCESS;
}

int mdhim_bgrm_error(const mdhim_bgrm_t *bgrm, int *error) {
    if (!bgrm || !bgrm->bgrm || !error) {
        return MDHIM_ERROR;
    }

    *error = bgrm->bgrm->error;
    return MDHIM_SUCCESS;
}

int mdhim_bgrm_keys(const mdhim_bgrm_t *bgrm, void ***keys, std::size_t **key_lens) {
    if (!bgrm || !bgrm->bgrm) {
        return MDHIM_ERROR;
    }

    if (keys) {
        if (!*keys) {
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < bgrm->bgrm->num_keys; i++) {
            (*keys)[i] = bgrm->bgrm->keys[i];
        }
    }

    if (key_lens) {
        for(std::size_t i = 0; i < bgrm->bgrm->num_keys; i++) {
            (*key_lens)[i] = bgrm->bgrm->key_lens[i];
        }
    }

    return MDHIM_SUCCESS;
}

int mdhim_bgrm_values(const mdhim_bgrm_t *bgrm, void ***values, std::size_t **value_lens) {
    if (!bgrm || !bgrm->bgrm) {
        return MDHIM_ERROR;
    }

    if (values) {
        if (!*values) {
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < bgrm->bgrm->num_keys; i++) {
            (*values)[i] = bgrm->bgrm->values[i];
        }
    }

    if (value_lens) {
        if (!*value_lens) {
            return MDHIM_ERROR;
        }

        for(std::size_t i = 0; i < bgrm->bgrm->num_keys; i++) {
            (*value_lens)[i] = bgrm->bgrm->value_lens[i];
        }
    }

    return MDHIM_SUCCESS;
}

int mdhim_bgrm_num_keys(const mdhim_bgrm_t *bgrm, std::size_t *num_keys) {
    if (!bgrm || !bgrm->bgrm || !num_keys) {
        return MDHIM_ERROR;
    }

    *num_keys = bgrm->bgrm->num_keys;
    return MDHIM_SUCCESS;
}

int mdhim_bgrm_next(const mdhim_bgrm_t *bgrm, mdhim_bgrm_t **next) {
    if (!bgrm || !bgrm->bgrm || !next) {
        return MDHIM_ERROR;
    }

    *next = nullptr;
    if (bgrm->bgrm->next) {
        *next = mdhim_bgrm_init(bgrm->bgrm->next);
    }
    return MDHIM_SUCCESS;
}
