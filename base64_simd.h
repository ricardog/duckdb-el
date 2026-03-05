#ifndef BASE64_SIMD_H
#define BASE64_SIMD_H

#include <stddef.h>
#include <stdint.h>

/* Returns the size of the encoded data (excluding null terminator) */
size_t base64_encode_simd(const unsigned char *src, size_t len, char *dst);

/* Returns the size of the decoded data. Returns (size_t)-1 on error. */
size_t base64_decode_simd(const char *src, size_t len, unsigned char *dst);

/* Helper to get required buffer sizes */
static inline size_t base64_encode_len(size_t len) {
    return ((len + 2) / 3) * 4;
}

static inline size_t base64_decode_len(size_t len) {
    return (len / 4) * 3;
}

#endif
