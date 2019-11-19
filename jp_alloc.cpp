#include <cstring>
#include <cstddef>
#include <atomic>
#include <new>

#include <sys/mman.h>
#include <errno.h>
#include <unistd.h>

#define DEBUG

#ifdef DEBUG
#include <iostream>
#include <fstream>
#endif

#ifdef __GNUC__
#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)
#else
#define likely(x)       (x)
#define unlikely(x)     (x)
#endif
	
#ifndef JP_ALLOC_POOL_COUNT
#define JP_ALLOC_POOL_COUNT 16
#endif

namespace {

size_t os_page_size() 
{ 
   return sysconf(_SC_PAGESIZE); 
}

void *os_alloc_pages(size_t size)
{
	void *mem = mmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (mem == MAP_FAILED) mem = nullptr;
        return mem;
}

void os_free_pages(void *mem, size_t size)
{
   munmap(mem, size);
}

union header
{
	struct {
		size_t size; // In principle, only size needs to be outside user area
		header *next;
	} s;
        std::max_align_t _align;
};
                                                    
struct pool
{

#ifdef DEBUG
   struct
   {
      std::atomic<unsigned long> alloc_calls;
      std::atomic<unsigned long> alloc_count;
      std::atomic<unsigned long> free_count;
   } stat = {};
#endif

   std::atomic<header*> head;
};

#ifdef DEBUG
struct {
      std::atomic<unsigned long> bad_free;
      std::atomic<unsigned long> jp_alloc;
      std::atomic<unsigned long> jp_alloc_aligned;
      std::atomic<unsigned long> jp_realloc;
      std::atomic<unsigned long> mallopt;
} stat = {};
#endif

pool g_pools[JP_ALLOC_POOL_COUNT] = {};
pool *g_pools_last = g_pools + JP_ALLOC_POOL_COUNT - 1;

void pool_put(header *h, pool *p)
{
#ifdef DEBUG
        p->stat.alloc_count--;
	p->stat.free_count++;
#endif
	header *expected = p->head;
	do h->s.next = expected;
	while (!p->head.compare_exchange_weak(expected, h));
}

void *pool_get(pool *p) 
{
	header *expected = p->head;
	if (likely(expected != nullptr)) {
                // Normal case. Grap memory from pool
		header *next;
		do next = expected->s.next;
		while (!p->head.compare_exchange_weak(expected, next) && expected != nullptr);
#ifdef DEBUG
                if (expected != nullptr) p->stat.alloc_count++, p->stat.free_count--;
#endif
	}
	if (unlikely(expected == nullptr)) {
                // Current pool was empty
		if (p == g_pools_last) {
                        // Last pool. Ask OS for memory
			constexpr size_t sz = 1U << (JP_ALLOC_POOL_COUNT - 1);
			expected = static_cast<header*>(os_alloc_pages(sz));
			if (likely(expected != nullptr)) expected->s.size = JP_ALLOC_POOL_COUNT - 1;
#ifdef DEBUG
                        if (expected != nullptr) p->stat.alloc_count++;
#endif
		}
		else {
                        // Get from next pool and split
			char *mem = static_cast<char*>(pool_get(p + 1));
                        if (mem != nullptr) {
                           expected = reinterpret_cast<header*>(mem);
                           size_t sz = expected->s.size - 1;
                           header *spare = reinterpret_cast<header*>(mem + (1U << sz));
                           expected->s.size = sz;
                           spare->s.size = sz;
#ifdef DEBUG
                           // one p+1 allocation becomes two allocated in p
                           (p+1)->stat.alloc_count--;
                           p->stat.alloc_count += 2;
#endif
                           pool_put(spare, p);
                        }
		}
	}
#ifdef DEBUG
        p->stat.alloc_calls++;
        if (expected) expected->s.next = expected;
#endif
	return expected;
}

size_t pool_id(size_t size)
{
   --size;
   size_t id = 0;
   while (size) ++id, size >>= 1;
   return id;
}

bool is_pow2(size_t n)
{
   // test if n is power of 2. true for 0 also
   return (n & (n - 1)) == 0;
}

void *alloc_pages_aligned(size_t alignment, size_t size)
{
   if (unlikely(!is_pow2(alignment))) return nullptr;
   const size_t ps = os_page_size();
   size_t pre_padding = 0; // space needed before header to align final pointer
   size_t align_size = 0; // extra space needed to ensure we can get correct alignment in span
   if (alignment > ps) {
      pre_padding = ps - sizeof(header);
      align_size = alignment - ps; // need alignment pages - 1 to ensure alignment
   }
   else if (alignment > sizeof(header)) {
      pre_padding = alignment - sizeof(header);
   }
   else {
      alignment = sizeof(header); // minimum alignment.
   }
   size_t span_size = pre_padding + size + align_size;
   size_t span_size_rounded = (span_size + ps - 1) & ~(ps - 1); // round to whole pages
   char *span = static_cast<char*>(os_alloc_pages(span_size_rounded));
   if (unlikely(span == nullptr)) return nullptr;
   char *hdr = span + pre_padding;
   size_t offset = (alignment - reinterpret_cast<size_t>(hdr + sizeof(header)) & (alignment - 1)) & (alignment - 1);
   hdr += offset;
   if (align_size > 0) {
      // if we have align pages, offset will be in whole pages (alignment > page size)
      // free pre and post align pages
      size_t pre_size = offset;
      size_t post_size = align_size - pre_size;
      if (pre_size > 0) os_free_pages(span, pre_size);
      if (post_size > 0) os_free_pages(span + span_size_rounded - post_size, post_size);
   }
   return hdr;
}


} // namespace

#ifdef DEBUG

void jpalloc_print_stats()
{
	std::ofstream out(std::string("/tmp/jpalloc.log-") + std::to_string(getpid()));
	out << "-------" << std::endl;
	out << "page size.......: " << os_page_size() << std::endl;
	out << "pool count......: " << JP_ALLOC_POOL_COUNT << std::endl;
	out << "bad free........: " << stat.bad_free << std::endl;
   	out << "jp_alloc........: " << stat.jp_alloc << std::endl;
   	out << "jp_alloc_aligned: " << stat.jp_alloc_aligned << std::endl;
   	out << "jp_realloc......: " << stat.jp_realloc << std::endl;
   	out << "mallopt.........: " << stat.mallopt << std::endl;
	for (size_t i = 0; i < JP_ALLOC_POOL_COUNT; ++i) {
		out << i << ": " << g_pools[i].stat.alloc_calls << ' ' << g_pools[i].stat.alloc_count << ' ' << g_pools[i].stat.free_count << std::endl;
	}
	out << "-------" << std::endl;
}
#endif

size_t jp_good_size(size_t size)
{
	size_t pid = pool_id(size);
	if (likely(pid < JP_ALLOC_POOL_COUNT)) {
		size = (1U << pid);
        }
        else {
                size_t ps_mask = os_page_size() - 1;
                size = (size + ps_mask) & ~ps_mask; // round to whole pages
        }
        return size - sizeof(header);
}
	
void jp_free(void *mem)
{
	if (unlikely(mem == nullptr)) return;
		
	header *h = static_cast<header*>(mem) - 1;
#ifdef DEBUG
	if (h->s.next != h) { ++stat.bad_free; return; }
#endif
	size_t size = h->s.size;
	if (likely(size < JP_ALLOC_POOL_COUNT)) {
		pool_put(h, g_pools + size);
	}
	else {
                size_t pre_padding = reinterpret_cast<size_t>(mem) & (os_page_size() - 1);
		os_free_pages(reinterpret_cast<char*>(h) + pre_padding, size + pre_padding);
	}
}

#ifdef DEBUG
static int _ae = std::atexit(jpalloc_print_stats);
#endif

void *jp_alloc(size_t size)
{
#ifdef DEBUG
        ++stat.jp_alloc;
#endif
	size += sizeof(header);
	void *mem;
	size_t pid = pool_id(size);
	if (likely(pid < JP_ALLOC_POOL_COUNT)) {
		mem = pool_get(g_pools + pid);
        }
        else {
                size_t ps_mask = os_page_size() - 1;
                size = (size + ps_mask) & ~ps_mask; // round to whole pages
		mem = os_alloc_pages(size);
		if (mem == nullptr) return nullptr;
		header *h = static_cast<header*>(mem);
		h->s.size = size;
#ifdef DEBUG
                h->s.next = h;
#endif
	}

	return static_cast<header*>(mem) + 1;
}

void *jp_alloc_aligned(size_t alignment, size_t size)
{
#ifdef DEBUG
        ++stat.jp_alloc_aligned;
#endif
	size += sizeof(header);
	void *mem = alloc_pages_aligned(alignment, size);
        if (mem == nullptr) return nullptr;
        header *h = static_cast<header*>(mem);
        h->s.size = size;
#ifdef DEBUG
        h->s.next = h;
#endif
	return static_cast<header*>(mem) + 1;
}

void *jp_calloc(size_t num, size_t nsize)
{
   size_t size = num * nsize;

   /* check mul overflow */
   if (num && nsize != size / num)  {
	   errno = ENOMEM;
	   return nullptr;
   }

   void *mem = jp_alloc(size);
   if (mem) memset(mem, 0, size);
   return mem;
}


void *jp_realloc(void *mem, size_t new_size)
{
   // todo: consider using mremap for large allocations
#ifdef DEBUG
        ++stat.jp_realloc;
#endif
        size_t size = 0;
        if (mem != nullptr) {
           header *h = static_cast<header*>(mem) - 1;
           size = h->s.size;
           if (size < JP_ALLOC_POOL_COUNT) size = 1U << size;
           size -= sizeof(header);
        }
        if (new_size > size) {
           void *new_mem = jp_alloc(new_size);
           if (new_mem) memcpy(new_mem, mem, size);
           jp_free(mem);
           mem = new_mem;
        }
        else if (new_size == 0) {
           jp_free(mem);
           mem = nullptr;
        }
	return mem;
}

extern "C" void *reallocarray(void *ptr, size_t nmemb, size_t size)
{
   size_t total_size = nmemb * size;

   // check mul overflow 
   if (nmemb && size != total_size / nmemb) {
	   errno = ENOMEM;
	   return nullptr;
   }
   return jp_realloc(ptr, total_size);
}


extern "C" int posix_memalign(void **memptr, size_t alignment, size_t size)
{
	void *mem = jp_alloc_aligned(alignment, size);
	if (mem == nullptr) return ENOMEM;
	*memptr = mem;
	return 0;
}


extern "C" size_t malloc_usable_size (void *ptr)
{
	header *h = reinterpret_cast<header*>(ptr) - 1;
        size_t size = h->s.size;
        if (size < JP_ALLOC_POOL_COUNT) size = 1U << size;
	return size - sizeof(header);
}


extern "C" int mallopt(int param, int value)
{
#ifdef DEBUG
	++stat.mallopt;
#endif
	return 0;
}

extern "C" void free(void *mem) { jp_free(mem); }
extern "C" void cfree(void *mem) { jp_free(mem); }
extern "C" void *malloc(size_t size) { return jp_alloc(size); }
extern "C" void *calloc(size_t num, size_t nsize) { return jp_calloc(num, nsize); }
extern "C" void *valloc(size_t size) { return jp_alloc_aligned(os_page_size(), size); }
extern "C" void *memalign(size_t alignment, size_t size) { return jp_alloc_aligned(alignment, size); }
extern "C" void *pvalloc(size_t size) { return jp_alloc_aligned(os_page_size(), size); }
extern "C" void *realloc(void *mem, size_t new_size) { return jp_realloc(mem, new_size); }
extern "C" void *aligned_alloc(size_t alignment, size_t size) { return jp_alloc_aligned(alignment, size); }
extern "C" size_t malloc_size (void *ptr) { return malloc_usable_size(ptr); }
extern "C" size_t malloc_good_size(size_t size) { return jp_good_size(size); }

// libc symbols
extern "C" void* __libc_malloc(size_t size) { return malloc(size); }
extern "C" void __libc_free(void* ptr) { jp_free(ptr); }
extern "C" void* __libc_realloc(void* ptr, size_t size) { return jp_realloc(ptr, size); }
extern "C" void* __libc_calloc(size_t n, size_t size) { return jp_calloc(n, size); }
extern "C" void __libc_cfree(void* ptr) { jp_free(ptr); }
extern "C" void* __libc_memalign(size_t align, size_t s) { return jp_alloc_aligned(align, s); }
extern "C" void* __libc_valloc(size_t size) { return malloc(size); }
extern "C" void* __libc_pvalloc(size_t size) { return malloc(size); }
extern "C" int __posix_memalign(void** r, size_t a, size_t s) { return posix_memalign(r, a, s); }


void * operator new(std::size_t n) { return malloc(n); }
void* operator new(size_t size, const std::nothrow_t& nt) noexcept { return malloc(size); }
void operator delete(void * p) noexcept { jp_free(p); }
void *operator new[](std::size_t s) { return malloc(s); }
void* operator new[](size_t size, const std::nothrow_t& nt) noexcept { return malloc(size); }
void operator delete[](void *p) noexcept { jp_free(p); }

// c++14
void operator delete(void* p, size_t s) noexcept  { jp_free(p); }
void operator delete[](void* p, size_t s) noexcept { jp_free(p); }


// aligned new, delete
#if 0 // c++17

namespace std { enum class align_val_t : std::size_t {}; } // todo

void* operator new(size_t size, std::align_val_t al) { return jp_alloc_aligned((size_t)al, size); }
void operator delete(void* p, std::align_val_t al) noexcept { jp_free(p); }
void* operator new[](size_t size, std::align_val_t al) { return jp_alloc_aligned((size_t)al, size); }
void operator delete[](void* p, std::align_val_t al) noexcept { jp_free(p); }
void* operator new(size_t size, std::align_val_t al, const std::nothrow_t& nt) noexcept { return jp_alloc_aligned((size_t)al, size); }
void* operator new[](size_t size, std::align_val_t al, const std::nothrow_t& nt) noexcept { return jp_alloc_aligned((size_t)al, size); }
void operator delete(void* ptr, std::align_val_t al, const std::nothrow_t& nt) noexcept { jp_free(ptr); }
void operator delete[](void* ptr, std::align_val_t al, const std::nothrow_t& nt) noexcept { jp_free(ptr); }
void operator delete(void* p, size_t s, std::align_val_t al) noexcept { jp_free(p); }
void operator delete[](void* p, size_t s, std::align_val_t al) noexcept { jp_free(p); }
#endif

#if 0 
// replace jemalloc

//extern "C" void replace_init(const malloc_table_t *table) { return; }
extern "C" void *replace_malloc(size_t size) { return jp_alloc(size); }
extern "C" int replace_posix_memalign(void **ptr, size_t alignment, size_t size) { return posix_memalign(ptr, alignment, size); }
extern "C" void *replace_aligned_alloc(size_t alignment, size_t size) { return jp_alloc_aligned(alignment, size); }
extern "C" void *replace_calloc(size_t num, size_t size) { return jp_calloc(num, size); }
extern "C" void *replace_realloc(void *ptr, size_t size) { return jp_realloc(ptr, size); }
extern "C" void replace_free(void *ptr) { jp_free(ptr); }
extern "C" void *replace_memalign(size_t alignment, size_t size) { return jp_alloc_aligned(alignment, size); }
extern "C" void *replace_valloc(size_t size) { return jp_alloc_aligned(os_page_size(), size); }
extern "C" size_t replace_malloc_usable_size(void* ptr) { return malloc_usable_size(ptr); }
extern "C" size_t replace_malloc_good_size(size_t size) { return jp_good_size(size); }
//extern "C" void replace_jemalloc_stats(jemalloc_stats_t *stats) { return; }
extern "C" void replace_jemalloc_purge_freed_pages() { return; }
extern "C" void replace_jemalloc_free_dirty_pages() { return; }
#endif
