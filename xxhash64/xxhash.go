package xxhash64

/*
#cgo CFLAGS: -O3
#cgo linux,arm64 CFLAGS: -march=armv8-a+simd
#cgo amd64 CFLAGS: -mavx2

#include <stdint.h>
#include <stddef.h>
#include <string.h>

#if defined(__x86_64__) || defined(_M_X64)
  #include <immintrin.h>
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
  #include <arm_neon.h>
  #if defined(__linux__) || defined(__ANDROID__)
    #include <sys/auxv.h>
    #include <asm/hwcap.h>
  #endif
#endif

// ======================
//    XXHASH CONSTANTS
// ======================
static const uint64_t XXH_PRIME64_1 = 11400714785074694791ULL;
static const uint64_t XXH_PRIME64_2 = 14029467366897019727ULL;
static const uint64_t XXH_PRIME64_3 =  1609587929392839161ULL;
static const uint64_t XXH_PRIME64_4 =  9650029242287828579ULL;
static const uint64_t XXH_PRIME64_5 =  2870177450012600261ULL;

static inline uint64_t xxh_rotl64(uint64_t v, int r) {
    return (v << r) | (v >> (64 - r));
}

static inline uint64_t xxh_round(uint64_t acc, uint64_t input) {
    acc += input * XXH_PRIME64_2;
    acc = xxh_rotl64(acc, 31);
    acc *= XXH_PRIME64_1;
    return acc;
}

static inline uint64_t xxh_merge_round(uint64_t acc, uint64_t val) {
    val = xxh_round(0, val);
    acc ^= val;
    acc = acc * XXH_PRIME64_1 + XXH_PRIME64_4;
    return acc;
}

static inline uint64_t xxh_read64(const void* p) {
    uint64_t v;
    memcpy(&v, p, sizeof(v));
    return v;
}
static inline uint32_t xxh_read32(const void* p) {
    uint32_t v;
    memcpy(&v, p, sizeof(v));
    return v;
}

static inline uint64_t xxh_finalize(uint64_t h, const uint8_t *p, size_t len) {
    const uint8_t *end = p + len;

    while ((p + 8) <= end) {
        uint64_t k1 = xxh_read64(p);
        k1 *= XXH_PRIME64_2;
        k1 = xxh_rotl64(k1, 31);
        k1 *= XXH_PRIME64_1;
        h ^= k1;
        h = xxh_rotl64(h, 27) * XXH_PRIME64_1 + XXH_PRIME64_4;
        p += 8;
    }

    if ((p + 4) <= end) {
        uint32_t k1_32 = xxh_read32(p);
        h ^= (uint64_t)k1_32 * XXH_PRIME64_1;
        h = xxh_rotl64(h, 23) * XXH_PRIME64_2 + XXH_PRIME64_3;
        p += 4;
    }

    while (p < end) {
        h ^= (*p) * XXH_PRIME64_5;
        h = xxh_rotl64(h, 11) * XXH_PRIME64_1;
        p++;
    }

    h ^= h >> 33;
    h *= XXH_PRIME64_2;
    h ^= h >> 29;
    h *= XXH_PRIME64_3;
    h ^= h >> 32;

    return h;
}

// ============================================================================
//   x86_64 AVX2 detection
// ============================================================================
static int cpu_supports_avx2() {
#if !(defined(__x86_64__) || defined(_M_X64))
    return 0;
#else
    unsigned int eax, ebx, ecx, edx;

    __asm__ volatile ("cpuid"
                      : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx)
                      : "a"(1), "c"(0));

    int osxsave = (ecx >> 27) & 1;
    int avx     = (ecx >> 28) & 1;
    if (!osxsave || !avx) return 0;

    unsigned int xcr0_lo, xcr0_hi;
    __asm__ volatile ("xgetbv"
                      : "=a"(xcr0_lo), "=d"(xcr0_hi)
                      : "c"(0));
    uint64_t xcr0 = ((uint64_t)xcr0_hi << 32) | xcr0_lo;
    if ((xcr0 & 0x6) != 0x6) return 0;

    __asm__ volatile ("cpuid"
                      : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx)
                      : "a"(7), "c"(0));

    return (ebx & (1u << 5)) != 0;
#endif
}

// ============================================================================
//   aarch64 NEON detection
// ============================================================================
static int cpu_supports_neon() {
#if defined(__aarch64__) || defined(_M_ARM64)
  return 1;
#else
  return 0;
#endif
}

// ============================================================================
//   Scalar baseline
// ============================================================================
static uint64_t xxhash64_scalar_seed(const void *input, size_t len, uint64_t seed) {
    const uint8_t *p   = (const uint8_t*)input;
    const uint8_t *end = p + len;
    uint64_t h;

    if (len >= 32) {
        const uint8_t *limit = end - 32;

        uint64_t v1 = seed + XXH_PRIME64_1 + XXH_PRIME64_2;
        uint64_t v2 = seed + XXH_PRIME64_2;
        uint64_t v3 = seed + 0;
        uint64_t v4 = seed - XXH_PRIME64_1;

        do {
            uint64_t s1 = xxh_read64(p + 0);
            uint64_t s2 = xxh_read64(p + 8);
            uint64_t s3 = xxh_read64(p + 16);
            uint64_t s4 = xxh_read64(p + 24);

            v1 = xxh_round(v1, s1);
            v2 = xxh_round(v2, s2);
            v3 = xxh_round(v3, s3);
            v4 = xxh_round(v4, s4);

            p += 32;
        } while (p <= limit);

        h = xxh_rotl64(v1, 1)
          + xxh_rotl64(v2, 7)
          + xxh_rotl64(v3, 12)
          + xxh_rotl64(v4, 18);

        h = xxh_merge_round(h, v1);
        h = xxh_merge_round(h, v2);
        h = xxh_merge_round(h, v3);
        h = xxh_merge_round(h, v4);
    } else {
        h = seed + XXH_PRIME64_5;
    }

    h += len;
    return xxh_finalize(h, p, (size_t)(end - p));
}

#if defined(__x86_64__) || defined(_M_X64)
// ============================================================================
//   AVX2 optimized - maximum performance
// ============================================================================

// Multiply two 64-bit values using AVX2 (low 64-bit result)
static inline __m256i xxh_mul64_avx2(__m256i a, __m256i b) {
    __m256i a_lo = _mm256_and_si256(a, _mm256_set1_epi64x(0xFFFFFFFFULL));
    __m256i a_hi = _mm256_srli_epi64(a, 32);
    __m256i b_lo = _mm256_and_si256(b, _mm256_set1_epi64x(0xFFFFFFFFULL));
    __m256i b_hi = _mm256_srli_epi64(b, 32);

    __m256i lo_lo = _mm256_mul_epu32(a_lo, b_lo);
    __m256i lo_hi = _mm256_mul_epu32(a_lo, b_hi);
    __m256i hi_lo = _mm256_mul_epu32(a_hi, b_lo);

    __m256i middle = _mm256_add_epi64(lo_hi, hi_lo);
    middle = _mm256_slli_epi64(middle, 32);

    return _mm256_add_epi64(lo_lo, middle);
}

static inline __m256i xxh_rotl64_avx2(__m256i v, int r) {
    return _mm256_or_si256(_mm256_slli_epi64(v, r), _mm256_srli_epi64(v, 64 - r));
}

static inline __m256i xxh_round_avx2(__m256i acc, __m256i input) {
    acc = _mm256_add_epi64(acc, xxh_mul64_avx2(input, _mm256_set1_epi64x(XXH_PRIME64_2)));
    acc = xxh_rotl64_avx2(acc, 31);
    acc = xxh_mul64_avx2(acc, _mm256_set1_epi64x(XXH_PRIME64_1));
    return acc;
}

static uint64_t xxhash64_avx2_seed(const void *input, size_t len, uint64_t seed) {
    const uint8_t *p   = (const uint8_t*)input;
    const uint8_t *end = p + len;
    uint64_t h;

    if (len >= 32) {
        const uint8_t *limit = end - 32;

        __m256i v = _mm256_set_epi64x(
            (int64_t)(seed - XXH_PRIME64_1),
            (int64_t)(seed + 0),
            (int64_t)(seed + XXH_PRIME64_2),
            (int64_t)(seed + XXH_PRIME64_1 + XXH_PRIME64_2)
        );

        // Unroll loop by 4 for better ILP
        while (p + 128 <= end) {
            __m256i b0 = _mm256_loadu_si256((const __m256i*)(p + 0));
            __m256i b1 = _mm256_loadu_si256((const __m256i*)(p + 32));
            __m256i b2 = _mm256_loadu_si256((const __m256i*)(p + 64));
            __m256i b3 = _mm256_loadu_si256((const __m256i*)(p + 96));

            v = xxh_round_avx2(v, b0);
            v = xxh_round_avx2(v, b1);
            v = xxh_round_avx2(v, b2);
            v = xxh_round_avx2(v, b3);

            p += 128;
        }

        // Handle remaining 32-byte blocks
        while (p <= limit) {
            __m256i b = _mm256_loadu_si256((const __m256i*)p);
            v = xxh_round_avx2(v, b);
            p += 32;
        }

        uint64_t v1 = (uint64_t)_mm256_extract_epi64(v, 0);
        uint64_t v2 = (uint64_t)_mm256_extract_epi64(v, 1);
        uint64_t v3 = (uint64_t)_mm256_extract_epi64(v, 2);
        uint64_t v4 = (uint64_t)_mm256_extract_epi64(v, 3);

        h = xxh_rotl64(v1, 1)
          + xxh_rotl64(v2, 7)
          + xxh_rotl64(v3, 12)
          + xxh_rotl64(v4, 18);

        h = xxh_merge_round(h, v1);
        h = xxh_merge_round(h, v2);
        h = xxh_merge_round(h, v3);
        h = xxh_merge_round(h, v4);
    } else {
        h = seed + XXH_PRIME64_5;
    }

    h += len;
    return xxh_finalize(h, p, (size_t)(end - p));
}
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
// ============================================================================
//   NEON (fixed for clang)
// ============================================================================
static inline uint64x2_t xxh_rotl64q(uint64x2_t v, const int r) {
#if defined(__clang__)
	int64x2_t left_shift  = vdupq_n_s64(r);
	int64x2_t right_shift = vdupq_n_s64(-(64 - r));
	uint64x2_t left  = vshlq_u64(v, left_shift);
	uint64x2_t right = vshlq_u64(v, right_shift);
	return vorrq_u64(left, right);
#else
	return vorrq_u64(vshlq_n_u64(v, r), vshrq_n_u64(v, 64 - r));
#endif
}

static inline uint64x2_t xxh_roundq(uint64x2_t acc, uint64x2_t input) {
    uint64_t a0 = vgetq_lane_u64(acc, 0);
    uint64_t a1 = vgetq_lane_u64(acc, 1);
    uint64_t i0 = vgetq_lane_u64(input, 0);
    uint64_t i1 = vgetq_lane_u64(input, 1);

    a0 += i0 * XXH_PRIME64_2;
    a1 += i1 * XXH_PRIME64_2;

    a0 = xxh_rotl64(a0, 31) * XXH_PRIME64_1;
    a1 = xxh_rotl64(a1, 31) * XXH_PRIME64_1;

    uint64x2_t res = vdupq_n_u64(0);
    res = vsetq_lane_u64(a0, res, 0);
    res = vsetq_lane_u64(a1, res, 1);
    return res;
}

static inline uint64x2_t xxh_load2x64(const uint8_t* p) {
    uint8x16_t b = vld1q_u8(p);
    return vreinterpretq_u64_u8(b);
}

static uint64_t xxhash64_neon_seed(const void *input, size_t len, uint64_t seed) {
    const uint8_t *p   = (const uint8_t*)input;
    const uint8_t *end = p + len;
    uint64_t h;

    if (len >= 32) {
        const uint8_t *limit = end - 32;

        uint64x2_t v12 = (uint64x2_t){ seed + XXH_PRIME64_1 + XXH_PRIME64_2, seed + XXH_PRIME64_2 };
        uint64x2_t v34 = (uint64x2_t){ seed + 0,                             seed - XXH_PRIME64_1 };

        for (; p <= limit; p += 32) {
            v12 = xxh_roundq(v12, xxh_load2x64(p + 0));
            v34 = xxh_roundq(v34, xxh_load2x64(p + 16));
        }

        uint64_t v1 = vgetq_lane_u64(v12, 0);
        uint64_t v2 = vgetq_lane_u64(v12, 1);
        uint64_t v3 = vgetq_lane_u64(v34, 0);
        uint64_t v4 = vgetq_lane_u64(v34, 1);

        h = xxh_rotl64(v1, 1)
          + xxh_rotl64(v2, 7)
          + xxh_rotl64(v3, 12)
          + xxh_rotl64(v4, 18);

        h = xxh_merge_round(h, v1);
        h = xxh_merge_round(h, v2);
        h = xxh_merge_round(h, v3);
        h = xxh_merge_round(h, v4);
    } else {
        h = seed + XXH_PRIME64_5;
    }

    h += len;
    return xxh_finalize(h, p, (size_t)(end - p));
}
#endif

// ============================================================================
//   Runtime dispatch
// ============================================================================
static int use_avx2 = 0;
static int use_neon = 0;

void xxhash64_init(void){
    use_avx2 = cpu_supports_avx2();
    use_neon = cpu_supports_neon();
}

// ============================================================================
//   Public API
// ============================================================================
uint64_t xxhash64_seed(const void *data, size_t len, uint64_t seed) {
    if (!data || len == 0) return 0;

#if defined(__x86_64__) || defined(_M_X64)
    if (use_avx2) return xxhash64_avx2_seed(data, len, seed);
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
    if (use_neon) return xxhash64_neon_seed(data, len, seed);
#endif

    return xxhash64_scalar_seed(data, len, seed);
}

uint64_t xxhash64(const void *data, size_t len) {
    return xxhash64_seed(data, len, 0ULL);
}

*/
import "C"
import (
	"hydrakv/envhandler"
	"unsafe"
)

type XXHash64 struct {
	seed uint64
}

var XXH *XXHash64

func init() {
	var seed uint64 = 0

	seed = *envhandler.ENV.XXHASH_SEED

	XXH = &XXHash64{
		seed: seed,
	}

	C.xxhash64_init()
}

func (xx *XXHash64) HashBytes(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}
	return uint64(
		C.xxhash64_seed(
			unsafe.Pointer(&b[0]),
			C.size_t(len(b)),
			C.uint64_t(xx.seed),
		),
	)
}

func (xx *XXHash64) HashString(s string) uint64 {
	if len(s) == 0 {
		return 0
	}
	ptr := unsafe.StringData(s)
	return uint64(
		C.xxhash64_seed(
			unsafe.Pointer(ptr),
			C.size_t(len(s)),
			C.uint64_t(xx.seed),
		),
	)
}

func (xx *XXHash64) Sum64(b []byte) uint64 {
	return xx.HashBytes(b)
}

func (xx *XXHash64) HashBytesSeed(b []byte, seed uint64) uint64 {
	if len(b) == 0 {
		return 0
	}
	return uint64(C.xxhash64_seed(unsafe.Pointer(&b[0]), C.size_t(len(b)), C.uint64_t(seed)))
}

func (xx *XXHash64) HashStringSeed(s string, seed uint64) uint64 {
	if len(s) == 0 {
		return 0
	}
	ptr := unsafe.StringData(s)
	return uint64(C.xxhash64_seed(unsafe.Pointer(ptr), C.size_t(len(s)), C.uint64_t(seed)))
}

func (xx *XXHash64) Sum64Seed(b []byte, seed uint64) uint64 {
	return xx.HashBytesSeed(b, seed)
}
