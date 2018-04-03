#include "transport_private.hpp"
#include "MemoryManagers.hpp"

mdhim_putm_t *mdhim_pm_init(TransportPutMessage *pm) {
    mdhim_putm_t *ret = Memory::FBP_MEDIUM::Instance().acquire<mdhim_putm_t>();
    if (!ret || !(ret->p = Memory::FBP_MEDIUM::Instance().acquire<mdhim_putm_private_t>())) {
        return nullptr;
    }

    ret->p->pm = pm;
    return ret;
}

mdhim_bputm_t *mdhim_bpm_init(TransportBPutMessage *bpm) {
    mdhim_bputm_t *ret = Memory::FBP_MEDIUM::Instance().acquire<mdhim_bputm_t>();
    if (!ret || !(ret->p = Memory::FBP_MEDIUM::Instance().acquire<mdhim_bputm_private_t>())) {
        return nullptr;
    }

    ret->p->bpm = bpm;
    return ret;
}

mdhim_getm_t *mdhim_gm_init(TransportGetMessage *gm) {
    mdhim_getm_t *ret = Memory::FBP_MEDIUM::Instance().acquire<mdhim_getm_t>();
    if (!ret || !(ret->p = Memory::FBP_MEDIUM::Instance().acquire<mdhim_getm_private_t>())) {
        return nullptr;
    }

    ret->p->gm = gm;
    return ret;
}

mdhim_bgetm_t *mdhim_bgm_init(TransportBGetMessage *bgm) {
    mdhim_bgetm_t *ret = Memory::FBP_MEDIUM::Instance().acquire<mdhim_bgetm_t>();
    if (!ret || !(ret->p = Memory::FBP_MEDIUM::Instance().acquire<mdhim_bgetm_private_t>())) {
        return nullptr;
    }

    ret->p->bgm = bgm;
    return ret;
}

mdhim_delm_t *mdhim_delm_init(TransportDeleteMessage *dm) {
    mdhim_delm_t *ret = Memory::FBP_MEDIUM::Instance().acquire<mdhim_delm_t>();
    if (!ret || !(ret->p = Memory::FBP_MEDIUM::Instance().acquire<mdhim_delm_private_t>())) {
        return nullptr;
    }

    ret->p->dm = dm;
    return ret;
}

mdhim_bdelm_t *mdhim_bdelm_init(TransportBDeleteMessage *bdm) {
    mdhim_bdelm_t *ret = Memory::FBP_MEDIUM::Instance().acquire<mdhim_bdelm_t>();
    if (!ret || !(ret->p = Memory::FBP_MEDIUM::Instance().acquire<mdhim_bdelm_private_t>())) {
        return nullptr;
    }

    ret->p->bdm = bdm;
    return ret;
}

mdhim_rm_t *mdhim_rm_init(TransportRecvMessage *rm) {
    mdhim_rm_t *ret = Memory::FBP_MEDIUM::Instance().acquire<mdhim_rm_t>();
    if (!ret || !(ret->p = Memory::FBP_MEDIUM::Instance().acquire<mdhim_rm_private_t>())) {
        return nullptr;
    }

    ret->p->rm = rm;
    return ret;
}

mdhim_getrm_t *mdhim_grm_init(TransportGetRecvMessage *grm) {
    mdhim_getrm_t *ret = Memory::FBP_MEDIUM::Instance().acquire<mdhim_getrm_t>();
    if (!ret || !(ret->p = Memory::FBP_MEDIUM::Instance().acquire<mdhim_getrm_private_t>())) {
        return nullptr;
    }

    ret->p->grm = grm;
    return ret;
}

mdhim_bgetrm_t *mdhim_bgrm_init(TransportBGetRecvMessage *bgrm) {
    mdhim_bgetrm_t *ret = Memory::FBP_MEDIUM::Instance().acquire<mdhim_bgetrm_t>();
    if (!ret || !(ret->p = Memory::FBP_MEDIUM::Instance().acquire<mdhim_bgetrm_private_t>())) {
        return nullptr;
    }

    ret->p->bgrm = bgrm;
    return ret;
}

mdhim_brm_t *mdhim_brm_init(TransportBRecvMessage *brm) {
    mdhim_brm_t *ret = Memory::FBP_MEDIUM::Instance().acquire<mdhim_brm_t>();
    if (!ret || !(ret->p = Memory::FBP_MEDIUM::Instance().acquire<mdhim_brm_private_t>())) {
        return nullptr;
    }

    ret->p->brm = brm;
    return ret;
}

void mdhim_putm_destroy(mdhim_putm_t *pm) {
    if (pm) {
        if (pm->p) {
            Memory::FBP_MEDIUM::Instance().release(pm->p->pm);
            Memory::FBP_MEDIUM::Instance().release(pm->p);
            pm->p = nullptr;
        }

        Memory::FBP_MEDIUM::Instance().release(pm);
    }
}

void mdhim_bputm_destroy(mdhim_bputm_t *bpm) {
    if (bpm) {
        if (bpm->p) {
            Memory::FBP_MEDIUM::Instance().release(bpm->p->bpm);
            Memory::FBP_MEDIUM::Instance().release(bpm->p);
            bpm->p = nullptr;
        }

        Memory::FBP_MEDIUM::Instance().release(bpm);
    }
}

void mdhim_getm_destroy(mdhim_getm_t *gm) {
    if (gm) {
        if (gm->p) {
            Memory::FBP_MEDIUM::Instance().release(gm->p->gm);
            Memory::FBP_MEDIUM::Instance().release(gm->p);
            gm->p = nullptr;
        }

        Memory::FBP_MEDIUM::Instance().release(gm);
    }
}

void mdhim_bgetm_destroy(mdhim_bgetm_t *bgm) {
    if (bgm) {
        if (bgm->p) {
            Memory::FBP_MEDIUM::Instance().release(bgm->p->bgm);
            Memory::FBP_MEDIUM::Instance().release(bgm->p);
            bgm->p = nullptr;
        }

        Memory::FBP_MEDIUM::Instance().release(bgm);
    }
}

void mdhim_delm_destroy(mdhim_delm_t *dm) {
    if (dm) {
        if (dm->p) {
            Memory::FBP_MEDIUM::Instance().release(dm->p->dm);
            Memory::FBP_MEDIUM::Instance().release(dm->p);
            dm->p = nullptr;
        }

        Memory::FBP_MEDIUM::Instance().release(dm);
    }
}

void mdhim_bdelm_destroy(mdhim_bdelm_t *bdm) {
    if (bdm) {
        if (bdm->p) {
            Memory::FBP_MEDIUM::Instance().release(bdm->p->bdm);
            Memory::FBP_MEDIUM::Instance().release(bdm->p);
            bdm->p = nullptr;
        }

        Memory::FBP_MEDIUM::Instance().release(bdm);
    }
}

void mdhim_rm_destroy(mdhim_rm_t *rm) {
    if (rm) {
        if (rm->p) {
            Memory::FBP_MEDIUM::Instance().release(rm->p->rm);
            Memory::FBP_MEDIUM::Instance().release(rm->p);
            rm->p = nullptr;
        }

        Memory::FBP_MEDIUM::Instance().release(rm);
    }
}

void mdhim_grm_destroy(mdhim_getrm_t *grm) {
    if (grm) {
        if (grm->p) {
            Memory::FBP_MEDIUM::Instance().release(grm->p->grm);
            Memory::FBP_MEDIUM::Instance().release(grm->p);
            grm->p = nullptr;
        }

        Memory::FBP_MEDIUM::Instance().release(grm);
    }
}

void mdhim_bgrm_destroy(mdhim_bgetrm_t *bgrm) {
    if (bgrm) {
        if (bgrm->p) {
            Memory::FBP_MEDIUM::Instance().release(bgrm->p->bgrm);
            Memory::FBP_MEDIUM::Instance().release(bgrm->p);
            bgrm->p = nullptr;
        }

        Memory::FBP_MEDIUM::Instance().release(bgrm);
    }
}

void mdhim_brm_destroy(mdhim_brm_t *brm) {
    if (brm) {
        if (brm->p) {
            Memory::FBP_MEDIUM::Instance().release(brm->p->brm);
            Memory::FBP_MEDIUM::Instance().release(brm->p);
            brm->p = nullptr;
        }

        Memory::FBP_MEDIUM::Instance().release(brm);
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
