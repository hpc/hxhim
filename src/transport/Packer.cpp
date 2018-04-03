#include "Packer.hpp"
#include "MemoryManagers.hpp"

void Packer::cleanup(void **buf, int *bufsize) {
    if (buf) {
        Memory::FBP_MEDIUM::Instance().release(*buf);
        *buf = nullptr;
    }

    if (bufsize) {
        *bufsize = 0;
    }
}
