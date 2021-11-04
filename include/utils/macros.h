#ifndef HELPER_MACROS_H
#define HELPER_MACROS_H

/**
 * Enum and String generation adapted from
 * https://stackoverflow.com/a/10966395
 * by Terrence M
 */
#define GENERATE_ENUM(prefix, name) prefix##_##name,
#define GENERATE_STR(prefix, name)  #prefix "_" #name,

#endif
