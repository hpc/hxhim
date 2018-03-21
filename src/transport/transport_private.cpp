#include "transport_private.hpp"

mdhim_putm_t *mdhim_pm_init(TransportPutMessage *pm) {
    mdhim_putm_t *ret = new mdhim_putm_t();
    if (!ret || !(ret->p = new mdhim_putm_private_t())) {
        return nullptr;
    }

    ret->p->pm = pm;
    return ret;
}

mdhim_bputm_t *mdhim_bpm_init(TransportBPutMessage *bpm) {
    mdhim_bputm_t *ret = new mdhim_bputm_t();
    if (!ret || !(ret->p = new mdhim_bputm_private_t())) {
        return nullptr;
    }

    ret->p->bpm = bpm;
    return ret;
}

mdhim_getm_t *mdhim_gm_init(TransportGetMessage *gm) {
    mdhim_getm_t *ret = new mdhim_getm_t();
    if (!ret || !(ret->p = new mdhim_getm_private_t())) {
        return nullptr;
    }

    ret->p->gm = gm;
    return ret;
}

mdhim_bgetm_t *mdhim_bgm_init(TransportBGetMessage *bgm) {
    mdhim_bgetm_t *ret = new mdhim_bgetm_t();
    if (!ret || !(ret->p = new mdhim_bgetm_private_t())) {
        return nullptr;
    }

    ret->p->bgm = bgm;
    return ret;
}

mdhim_delm_t *mdhim_delm_init(TransportDeleteMessage *dm) {
    mdhim_delm_t *ret = new mdhim_delm_t();
    if (!ret || !(ret->p = new mdhim_delm_private_t())) {
        return nullptr;
    }

    ret->p->dm = dm;
    return ret;
}

mdhim_bdelm_t *mdhim_bdelm_init(TransportBDeleteMessage *bdm) {
    mdhim_bdelm_t *ret = new mdhim_bdelm_t();
    if (!ret || !(ret->p = new mdhim_bdelm_private_t())) {
        return nullptr;
    }

    ret->p->bdm = bdm;
    return ret;
}

mdhim_rm_t *mdhim_rm_init(TransportRecvMessage *rm) {
    mdhim_rm_t *ret = new mdhim_rm_t();
    if (!ret || !(ret->p = new mdhim_rm_private_t())) {
        return nullptr;
    }

    ret->p->rm = rm;
    return ret;
}

mdhim_getrm_t *mdhim_grm_init(TransportGetRecvMessage *grm) {
    mdhim_getrm_t *ret = new mdhim_getrm_t();
    if (!ret || !(ret->p = new mdhim_getrm_private_t())) {
        return nullptr;
    }

    ret->p->grm = grm;
    return ret;
}

mdhim_bgetrm_t *mdhim_bgrm_init(TransportBGetRecvMessage *bgrm) {
    mdhim_bgetrm_t *ret = new mdhim_bgetrm_t();
    if (!ret || !(ret->p = new mdhim_bgetrm_private_t())) {
        return nullptr;
    }

    ret->p->bgrm = bgrm;
    return ret;
}

mdhim_brm_t *mdhim_brm_init(TransportBRecvMessage *brm) {
    mdhim_brm_t *ret = new mdhim_brm_t();
    if (!ret || !(ret->p = new mdhim_brm_private_t())) {
        return nullptr;
    }

    ret->p->brm = brm;
    return ret;
}

void mdhim_putm_destroy(mdhim_putm_t *pm) {
    if (pm) {
        if (pm->p) {
            if (pm->p->pm) {
                delete pm->p->pm;
            }

            delete pm->p;
        }

        delete pm;
    }
}

void mdhim_bputm_destroy(mdhim_bputm_t *bpm) {
    if (bpm) {
        if (bpm->p) {
            if (bpm->p->bpm) {
                delete bpm->p->bpm;
            }

            delete bpm->p;
        }

        delete bpm;
    }
}

void mdhim_getm_destroy(mdhim_getm_t *gm) {
    if (gm) {
        if (gm->p) {
            if (gm->p->gm) {
                delete gm->p->gm;
            }

            delete gm->p;
        }

        delete gm;
    }
}

void mdhim_bgetm_destroy(mdhim_bgetm_t *bgm) {
    if (bgm) {
        if (bgm->p) {
            if (bgm->p->bgm) {
                delete bgm->p->bgm;
            }

            delete bgm->p;
        }

        delete bgm;
    }
}

void mdhim_delm_destroy(mdhim_delm_t *dm) {
    if (dm) {
        if (dm->p) {
            if (dm->p->dm) {
                delete dm->p->dm;
            }

            delete dm->p;
        }

        delete dm;
    }
}

void mdhim_bdelm_destroy(mdhim_bdelm_t *bdm) {
    if (bdm) {
        if (bdm->p) {
            if (bdm->p->bdm) {
                delete bdm->p->bdm;
            }

            delete bdm->p;
            bdm->p = nullptr;
        }

        delete bdm;
    }
}

void mdhim_rm_destroy(mdhim_rm_t *rm) {
    if (rm) {
        if (rm->p) {
            if (rm->p->rm) {
                delete rm->p->rm;
            }

            delete rm->p;
        }

        delete rm;
    }
}

void mdhim_grm_destroy(mdhim_getrm_t *grm) {
    if (grm) {
        if (grm->p) {
            if (grm->p->grm) {
                delete grm->p->grm;
            }

            delete grm->p;
        }

        delete grm;
    }
}

void mdhim_bgrm_destroy(mdhim_bgetrm_t *bgrm) {
    if (bgrm) {
        if (bgrm->p) {
            if (bgrm->p->bgrm) {
                delete bgrm->p->bgrm;
            }

            delete bgrm->p;
        }

        delete bgrm;
    }
}

void mdhim_brm_destroy(mdhim_brm_t *brm) {
    if (brm) {
        if (brm->p) {
            if (brm->p->brm) {
                delete brm->p->brm;
            }

            delete brm->p;
        }

        delete brm;
    }
}

int mdhim_brm_error(const mdhim_brm_t *brm, int *error) {
    if (!brm || !brm->p || !brm->p->brm || !error) {
        return MDHIM_ERROR;
    }

    *error = brm->p->brm->error;
    return MDHIM_SUCCESS;
}

int mdhim_grm_error(const mdhim_getrm_t *grm, int *error) {
    if (!grm || !grm->p || !grm->p->grm || !error) {
        return MDHIM_ERROR;
    }

    *error = grm->p->grm->error;
    return MDHIM_SUCCESS;
}

int  mdhim_grm_key(const mdhim_getrm_t *grm, void **key, int *key_len) {
    if (!grm || !grm->p || !grm->p->grm || !key || key_len) {
        return MDHIM_ERROR;
    }

    if (key) {
        *key = grm->p->grm->key;
    }

    if (key_len) {
        *key_len = grm->p->grm->key_len;
    }

    return MDHIM_SUCCESS;
}

int mdhim_grm_value(const mdhim_getrm_t *grm, void **value, int *value_len) {
    if (!grm || !grm->p || !grm->p->grm || !value || value_len) {
        return MDHIM_ERROR;
    }

    if (value) {
        *value = grm->p->grm->value;
    }

    if (value_len) {
        *value_len = grm->p->grm->value_len;
    }

    return MDHIM_SUCCESS;
}

int mdhim_bgrm_error(const mdhim_bgetrm_t *bgrm, int *error) {
    if (!bgrm || !bgrm->p || !bgrm->p->bgrm || !error) {
        return MDHIM_ERROR;
    }

    *error = bgrm->p->bgrm->error;
    return MDHIM_SUCCESS;
}

int  mdhim_bgrm_keys(const mdhim_bgetrm_t *bgrm, void ***keys, int **key_lens) {
    if (!bgrm || !bgrm->p || !bgrm->p->bgrm || !keys || key_lens) {
        return MDHIM_ERROR;
    }

    if (keys) {
        *keys = bgrm->p->bgrm->keys;
    }

    if (key_lens) {
        *key_lens = bgrm->p->bgrm->key_lens;
    }

    return MDHIM_SUCCESS;
}

int mdhim_bgrm_values(const mdhim_bgetrm_t *bgrm, void ***values, int **value_lens) {
    if (!bgrm || !bgrm->p || !bgrm->p->bgrm || !values || value_lens) {
        return MDHIM_ERROR;
    }

    if (values) {
        *values = bgrm->p->bgrm->values;
    }

    if (value_lens) {
        *value_lens = bgrm->p->bgrm->value_lens;
    }

    return MDHIM_SUCCESS;
}

int mdhim_bgrm_num_keys(const mdhim_bgetrm_t *bgrm, int *num_keys) {
    if (!bgrm || !bgrm->p || !bgrm->p->bgrm || !num_keys) {
        return MDHIM_ERROR;
    }

    *num_keys = bgrm->p->bgrm->num_keys;
    return MDHIM_SUCCESS;
}
