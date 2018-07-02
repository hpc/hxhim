
#include <iostream>
#include <string>
#include <hxhim/hxhim.h>
#include "demo/mesh.h"
using namespace std;


Mesh* create_mesh(int xdim, int ydim, int maxlevels) {
    // Create a level array mesh
    Mesh *m = new Mesh(xdim, ydim, maxlevels);
    return m;
}

void write_mesh(string name, Mesh* m) {
}

void read_mesh(string name, Mesh* m) {
}

int main(int argc, char** argv) {

    cout << "This is the HXHim Demo!" << endl;

    Mesh* m1 = create_mesh(100, 100, 1);
    //write_mesh("Mesh1", m1);

    Mesh* m2 = create_mesh(4, 4, 1);
    //read_mesh("Mesh2", m2);

    int cmp = 1;
    if (*m1 == *m2) {
        cout << "SUCCESS: Mesh M1 and M2 were the same." << endl;
        cmp = 0;
    }
    else {
        cout << "ERROR: Meshes M1 and M2 differed." << endl;
    }
    return cmp;
}
