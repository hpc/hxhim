
#include <iostream>
#include <string>
#include <hxhim.h>
using namespace std;

struct mesh {
};

mesh* create_mesh() {
    return 0;
}

int populate_mesh(mesh* m) {
  return -1;
}
void write_mesh(string name, mesh* m) {
}

void read_mesh(string name, mesh* m) {
}

int compare_mesh(mesh* m1, mesh* m2) {
    return -1;
}

int main(int argc, char** argv) {

    cout << "This is the HXHim Demo!" << endl;

    mesh* m1 = create_mesh();
    populate_mesh(m1);
    write_mesh("mesh_data", m1);

    mesh* m2 = create_mesh();
    read_mesh("mesh_data", m2);

    int cmp = compare_mesh(m1, m2);

    if (cmp == 0) {
        cout << "SUCCESS: Mesh M1 and M2 were the same." << endl;
    }
    else {
        cout << "ERROR: Meshes M1 and M2 differed." << endl;
    }
    return cmp;
}
