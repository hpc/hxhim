#include <cstring>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "rm_r.hpp"

void rm_r(const std::string &path, const char sep) {
    // checking for . and .. to be deleted here
    // would allow for entries that end with them
    // to be skipped erroneously without using
    // basename(3).
    //
    // This also allows for . and .. to be deleted.
    struct stat st;
    if (stat(path.c_str(), &st) == 0) {
        if (S_ISDIR(st.st_mode)) {

            DIR *dir = opendir(path.c_str());

            struct dirent *entry = nullptr;
            while ((entry = readdir(dir))) {
                const std::size_t len = strlen(entry->d_name);
                if (((len == 1) && (memcmp(entry->d_name, ".",  1) == 0))  ||
                    ((len == 2) && (memcmp(entry->d_name, "..", 2) == 0)))  {
                    continue;
                }

                const std::string sub = path + sep + entry->d_name;
                rm_r(sub, sep);
            }

            closedir(dir);
        }
    }

    remove(path.c_str());
}
