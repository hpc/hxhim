//
// Created by bws on 3/5/18.
//

#ifndef HXHIM_DEMO_MESH_H
#define HXHIM_DEMO_MESH_H

#include <cstdlib>
#include <vector>
#include "meshgen.h"

struct Cell {
    double x;
    double y;
    double temp;
    double pressure;
};

// Wrapper class to put a C++ interface on well-tested meshgen implementation
class Mesh {
public:
    enum RefinementStrategy {None = 0, Random};

    /** Create a level 0 mesh of xdim x ydim */
    Mesh(std::size_t xdim, std::size_t ydim, std::size_t maxlevels);

    /** @return The cell at index i,j */
    const Cell* Lookup(std::size_t i, std::size_t j) const;

    /** @return a vector of the cells at the requested refinement level */
    const std::vector<Cell> GetLevel(std::size_t level) const;

    /** @return 0 if this and other are equal */
    int Compare(const Mesh& other) const;

private:
    /** Meshgen non-opaque type */
    cell_list cells_;

};

inline int operator==(const Mesh& lhs, const Mesh& rhs) {
    if (lhs.Compare(rhs) == 0)
        return 1;
    else
        return 0;
}

#endif //HXHIM_MESH_H
