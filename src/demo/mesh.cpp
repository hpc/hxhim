//
// Created by bws on 4/17/18.
//

#include "mesh.h"
#include <cassert>

Mesh::Mesh(std::size_t xdim, std::size_t ydim, std::size_t maxlevels) {
    // Use the meshgen code to create a mesh of doubles
    // Several of the creation functions don't make any sense, but
    // this one seems okay except for the clunky types
    size_t leveldiff = 1;
    uint length = 1000;
    double sparsity = 0.1;
    size_t minbase = 2;
    cells_ = mesh_maker(cells_, leveldiff, &length, (uint*)&maxlevels, sparsity, minbase);
}

int Mesh::Refine(RefinementStrategy rs) {
    assert(rs == Mesh::Random);
    if (rs == Mesh::Random) {
        cells_ = adaptiveMeshConstructorWij(cells_, )
    }
    return 0;
}