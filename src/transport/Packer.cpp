#include "Packer.hpp"

void Packer::cleanup(void **buf, int *bufsize) {
    if (buf) {
        ::operator delete (*buf);
        *buf = nullptr;
    }

    if (bufsize) {
        *bufsize = 0;
    }
}
