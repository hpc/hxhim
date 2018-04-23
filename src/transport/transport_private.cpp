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

mdhim_getrm_t *mdhim_grm_init(TransportGetRecvMessage *grm) {
    if (!grm) {
        return nullptr;
    }

    mdhim_getrm_t *ret = new mdhim_getrm_t();
    if (!ret) {
        return nullptr;
    }

    ret->grm = grm;
    return ret;
}

mdhim_bgetrm_t *mdhim_bgrm_init(TransportBGetRecvMessage *bgrm) {
    if (!bgrm) {
        return nullptr;
    }

    mdhim_bgetrm_t *ret = new mdhim_bgetrm_t();
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

void mdhim_grm_destroy(mdhim_getrm_t *grm) {
    if (grm) {
        delete grm->grm;
    }

    delete grm;
}

void mdhim_bgrm_destroy(mdhim_bgetrm_t *bgrm) {
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

int mdhim_brm_error(const mdhim_brm_t *brm, int *error) {
    if (!brm || !brm->brm || !error) {
        return MDHIM_ERROR;
    }

    *error = brm->brm->error;
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

int mdhim_grm_src(const mdhim_getrm_t *grm, int *src) {
    if (!grm || !grm->grm || !src) {
        return MDHIM_ERROR;
    }

    *src = grm->grm->src;
    return MDHIM_SUCCESS;
}

int mdhim_grm_error(const mdhim_getrm_t *grm, int *error) {
    if (!grm || !grm->grm || !error) {
        return MDHIM_ERROR;
    }

    *error = grm->grm->error;
    return MDHIM_SUCCESS;
}

int mdhim_grm_key(const mdhim_getrm_t *grm, void **key, int *key_len) {
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

int mdhim_grm_value(const mdhim_getrm_t *grm, void **value, int *value_len) {
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

int mdhim_bgrm_src(const mdhim_bgetrm_t *bgrm, int *src) {
    if (!bgrm || !bgrm->bgrm || !src) {
        return MDHIM_ERROR;
    }

    *src = bgrm->bgrm->src;
    return MDHIM_SUCCESS;
}

int mdhim_bgrm_error(const mdhim_bgetrm_t *bgrm, int *error) {
    if (!bgrm || !bgrm->bgrm || !error) {
        return MDHIM_ERROR;
    }

    *error = bgrm->bgrm->error;
    return MDHIM_SUCCESS;
}

int  mdhim_bgrm_keys(const mdhim_bgetrm_t *bgrm, void ***keys, int **key_lens) {
    if (!bgrm || !bgrm->bgrm) {
        return MDHIM_ERROR;
    }

    if (keys) {
        *keys = bgrm->bgrm->keys;
    }

    if (key_lens) {
        *key_lens = bgrm->bgrm->key_lens;
    }

    return MDHIM_SUCCESS;
}

int mdhim_bgrm_values(const mdhim_bgetrm_t *bgrm, void ***values, int **value_lens) {
    if (!bgrm || !bgrm->bgrm) {
        return MDHIM_ERROR;
    }

    if (values) {
        *values = bgrm->bgrm->values;
    }

    if (value_lens) {
        *value_lens = bgrm->bgrm->value_lens;
    }

    return MDHIM_SUCCESS;
}

int mdhim_bgrm_num_keys(const mdhim_bgetrm_t *bgrm, int *num_keys) {
    if (!bgrm || !bgrm->bgrm || !num_keys) {
        return MDHIM_ERROR;
    }

    *num_keys = bgrm->bgrm->num_keys;
    return MDHIM_SUCCESS;
}

int mdhim_bgrm_next(const mdhim_bgetrm_t *bgrm, mdhim_bgetrm_t **next) {
    if (!bgrm || !bgrm->bgrm || !next) {
        return MDHIM_ERROR;
    }

    *next = nullptr;
    if (bgrm->bgrm->next) {
        *next = mdhim_bgrm_init(bgrm->bgrm->next);
    }
    return MDHIM_SUCCESS;
}
