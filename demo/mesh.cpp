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

    // Create a fixed dimension mesh
    //cells_ = mesh_maker(cells_, leveldiff, &length, (uint*)&maxlevels, sparsity, minbase);

    // Create a randomly refined mesh
    float threshhold = 0.5;
    cells_ = adaptiveMeshConstructorWij(cells_, xdim*ydim, maxlevels, threshhold, xdim*ydim*8);
}

int Mesh::Compare(const Mesh& other) const {
    return -1;
}
