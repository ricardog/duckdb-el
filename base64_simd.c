#include "base64_simd.h"
#include <string.h>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#elif defined(__aarch64__) || defined(_M_ARM64)
#include <arm_neon.h>
#endif

static const char *base64_table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static inline size_t
base64_encode_scalar(const unsigned char *src, size_t len, char *dst) {
    size_t i = 0, j = 0;
    while (i + 3 <= len) {
        uint32_t n = ((uint32_t)src[i] << 16) | ((uint32_t)src[i+1] << 8) | (uint32_t)src[i+2];
        dst[j++] = base64_table[(n >> 18) & 63];
        dst[j++] = base64_table[(n >> 12) & 63];
        dst[j++] = base64_table[(n >> 6) & 63];
        dst[j++] = base64_table[n & 63];
        i += 3;
    }
    if (i < len) {
        uint32_t n = (uint32_t)src[i] << 16;
        if (i + 1 < len) n |= (uint32_t)src[i+1] << 8;
        dst[j++] = base64_table[(n >> 18) & 63];
        dst[j++] = base64_table[(n >> 12) & 63];
        dst[j++] = (i + 1 < len) ? base64_table[(n >> 6) & 63] : '=';
        dst[j++] = '=';
    }
    dst[j] = '\0';
    return j;
}

#if defined(__aarch64__) || defined(_M_ARM64)
/* ARM64 NEON table lookup helper */
static inline uint8x16_t lookup_neon(uint8x16_t indices, uint8x16_t t0, uint8x16_t t1, uint8x16_t t2, uint8x16_t t3) {
    uint8x16_t r;
    r = vqtbl1q_u8(t0, indices);
    r = vorrq_u8(r, vqtbl1q_u8(t1, vsubq_u8(indices, vdupq_n_u8(16))));
    r = vorrq_u8(r, vqtbl1q_u8(t2, vsubq_u8(indices, vdupq_n_u8(32))));
    r = vorrq_u8(r, vqtbl1q_u8(t3, vsubq_u8(indices, vdupq_n_u8(48))));
    return r;
}

/* ARM64 NEON optimized encoding */
static inline size_t
base64_encode_neon(const unsigned char *src, size_t len, char *dst) {
    size_t i = 0, j = 0;
    uint8x16_t table0 = vld1q_u8((const uint8_t*)base64_table);
    uint8x16_t table1 = vld1q_u8((const uint8_t*)base64_table + 16);
    uint8x16_t table2 = vld1q_u8((const uint8_t*)base64_table + 32);
    uint8x16_t table3 = vld1q_u8((const uint8_t*)base64_table + 48);
    uint8x16_t mask3 = vdupq_n_u8(0x03);
    uint8x16_t maskF = vdupq_n_u8(0x0F);

    while (i + 48 <= len) {
        uint8x16x3_t in = vld3q_u8(src + i);
        
        uint8x16_t c0 = vshrq_n_u8(in.val[0], 2);
        uint8x16_t c1 = vorrq_u8(vshlq_n_u8(vandq_u8(in.val[0], mask3), 4), vshrq_n_u8(in.val[1], 4));
        uint8x16_t c2 = vorrq_u8(vshlq_n_u8(vandq_u8(in.val[1], maskF), 2), vshrq_n_u8(in.val[2], 6));
        uint8x16_t c3 = vandq_u8(in.val[2], vdupq_n_u8(0x3F));
        
        uint8x16x4_t out;
        out.val[0] = lookup_neon(c0, table0, table1, table2, table3);
        out.val[1] = lookup_neon(c1, table0, table1, table2, table3);
        out.val[2] = lookup_neon(c2, table0, table1, table2, table3);
        out.val[3] = lookup_neon(c3, table0, table1, table2, table3);
        
        vst4q_u8((uint8_t*)(dst + j), out);
        
        i += 48;
        j += 64;
    }
    return j + base64_encode_scalar(src + i, len - i, dst + j);
}
#endif

size_t
base64_encode_simd(const unsigned char *src, size_t len, char *dst) {
#if defined(__aarch64__) || defined(_M_ARM64)
    if (len >= 48) {
        return base64_encode_neon(src, len, dst);
    }
#endif
    return base64_encode_scalar(src, len, dst);
}

static const signed char decoding_table[256] = {
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63,
    52,53,54,55,56,57,58,59,60,61,-1,-1,-1, 0,-1,-1,
    -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,
    15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1,
    -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,
    41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
    -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
};

size_t
base64_decode_simd(const char *src, size_t len, unsigned char *dst) {
    if (len == 0) return 0;
    if (len % 4 != 0) return (size_t)-1;
    
    size_t j = 0;
    for (size_t i = 0; i < len; i += 4) {
        signed char a = decoding_table[(unsigned char)src[i]];
        signed char b = decoding_table[(unsigned char)src[i+1]];
        signed char c = decoding_table[(unsigned char)src[i+2]];
        signed char d = decoding_table[(unsigned char)src[i+3]];
        
        if (a == -1 || b == -1 || (src[i+2] != '=' && c == -1) || (src[i+3] != '=' && d == -1)) {
            return (size_t)-1;
        }
        
        uint32_t triple = ((uint32_t)(a & 0x3F) << 18) | 
                          ((uint32_t)(b & 0x3F) << 12) | 
                          ((uint32_t)(c & 0x3F) << 6) | 
                          (uint32_t)(d & 0x3F);
        
        dst[j++] = (triple >> 16) & 0xFF;
        if (src[i+2] != '=') dst[j++] = (triple >> 8) & 0xFF;
        if (src[i+3] != '=') dst[j++] = triple & 0xFF;
    }
    return j;
}
