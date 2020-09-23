#ifndef TRANSPORT_MKDIR_P_HPP
#define TRANSPORT_MKDIR_P_HPP

#include <string>
#include <sys/types.h>

int mkdir_p(const std::string & path, const mode_t mode, const char sep = '/');

#endif
