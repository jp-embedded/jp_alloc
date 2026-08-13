/* jp_alloc_cpp.cpp - C++ operator new/delete overrides for jp_alloc
 *
 * Links jp_alloc's malloc-family API to C++ global operator new/delete so
 * that all C++ allocations (new, new[], delete, delete[]) route through
 * jp_alloc's pools directly — not via the libc malloc override.
 *
 * Build: g++ -std=c++17 -fpic -c -O2 jp_alloc_cpp.cpp -o jp_alloc_cpp.o
 * Link into jp_alloc.so alongside jp_alloc.o.
 *
 * C-only programs do not need this file.
 */

#include <stddef.h>

extern "C" {
void *jp_alloc(size_t size);
void  jp_free(void *mem);
}

/* C++11: basic new/delete */
void *operator new(size_t size) { return jp_alloc(size); }
void operator delete(void *mem) noexcept { jp_free(mem); }
void *operator new[](size_t size) { return jp_alloc(size); }
void operator delete[](void *mem) noexcept { jp_free(mem); }

/* C++14: sized delete (size hint ignored — jp_alloc stores it in the header) */
void operator delete(void *mem, size_t) noexcept { jp_free(mem); }
void operator delete[](void *mem, size_t) noexcept { jp_free(mem); }

/* C++17: aligned new/delete (alignment hint ignored — jp_alloc returns
 * 16-byte-aligned blocks by default, which satisfies most alignments) */
#if __cplusplus >= 201703L
#include <new>
void *operator new(size_t size, std::align_val_t) { return jp_alloc(size); }
void operator delete(void *mem, std::align_val_t) noexcept { jp_free(mem); }
void *operator new[](size_t size, std::align_val_t) { return jp_alloc(size); }
void operator delete[](void *mem, std::align_val_t) noexcept { jp_free(mem); }
void operator delete(void *mem, size_t, std::align_val_t) noexcept { jp_free(mem); }
void operator delete[](void *mem, size_t, std::align_val_t) noexcept { jp_free(mem); }
#endif