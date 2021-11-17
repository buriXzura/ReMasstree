#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <math.h>
#include <iostream>
#include <mutex>
#include <atomic>
#include <assert.h>
#include <emmintrin.h>


#define REBAL
#define DRAM
//#define STATS

namespace masstree
{

#define LEAF_WIDTH          15
#define LEAF_THRESHOLD      1

#define INITIAL_VALUE       0x0123456789ABCDE0ULL
#define FULL_VALUE          0xEDCBA98765432100ULL

#define LV_BITS             (1ULL << 0)
#define IS_LV(x)            ((uintptr_t)x & LV_BITS)
#define LV_PTR(x)           (leafvalue*)((void*)((uintptr_t)x & ~LV_BITS))
#define SET_LV(x)           ((void*)((uintptr_t)x | LV_BITS))


#define INSERT_LOCK 0b1ULL
#define SMO_LOCK 0b10ULL
#define BOTH_LOCKS 0b11ULL
#define IS_ROOT 0b100ULL
#define IS_LEAF 0b1000ULL
#define INSERT_VERSION 0xfffff0ULL
#define SMO_VERSION 0xfffff000000ULL
#define LOCK_VERSION 0xfffff00000000000ULL
#define INSERT_RESET ~INSERT_VERSION
#define SMO_RESET ~SMO_VERSION
#define LOCK_RESET ~LOCK_VERSION
#define MAX_VERSION 0xfffff
#define INSERT_INCREMENT 0x10ULL
#define SMO_INCREMENT 0x1000000ULL



class VersionNumber {
public:
    uint64_t v;
    VersionNumber();
    VersionNumber(uint64_t v_) {
        v=v_;
    }
    void operator=(uint64_t v_) {
        v=v_;
    }
    void markRoot() {
        __sync_or_and_fetch(&v,IS_ROOT);
    }
    void unmarkRoot() {
        __sync_and_and_fetch(&v, ~IS_ROOT);
    }
    void markLeaf() {
        __sync_or_and_fetch(&v,IS_LEAF);
    }
    void unmarkLeaf() {
        __sync_and_and_fetch(&v,~IS_LEAF);
    }
    uint64_t tryInsertLock() {
        return (__sync_fetch_and_or(&v,INSERT_LOCK))&INSERT_LOCK;
    }
    void releaseInsertLock() {
        __sync_fetch_and_and(&v,~INSERT_LOCK);
    }
    uint64_t trySMOLock() {
        return (__sync_fetch_and_or(&v,BOTH_LOCKS))&SMO_LOCK;
    }
    void releaseSMOLock() {
        incrementSMO();
        __sync_fetch_and_and(&v,~SMO_LOCK);
    }
    void releaseBothLocks() {
        incrementInsert(), incrementSMO();
        __sync_fetch_and_and(&v,~BOTH_LOCKS);
    }
    bool smoLock() {
        return v&SMO_LOCK;
    }
    bool insertLock() {
        return v&INSERT_LOCK;
    }
    bool isRoot() {
        return v&IS_ROOT;
    }
    bool isLeaf() {
        return v&IS_LEAF;
    }
    uint insertVersion() {
        return (v&INSERT_VERSION)>>4;
    }
    uint smoVersion() {
        return (v&SMO_VERSION)>>24;
    }
    uint lockVersion() {
        return (v&LOCK_VERSION)>>44;
    }
    void incrementInsert() {
        if(insertVersion()==MAX_VERSION)
            __sync_fetch_and_and(&v,INSERT_RESET);
        else 
            __sync_fetch_and_add(&v,INSERT_INCREMENT);
    }
    void incrementSMO() {
        if(smoVersion()==MAX_VERSION)
            __sync_fetch_and_and(&v,~(SMO_RESET));
        else 
            __sync_fetch_and_add(&v,SMO_INCREMENT);
    }
    bool repair_req();
    uint updateLock();
    bool operator!= (VersionNumber const &a) {
        return v!=a.v;
    }
};


class permuter 
{

    /* the permutation's functionalities */

    public:
        permuter() {
            x_ = 0ULL;
        }

        permuter(uint64_t x) : x_(x) {
        }

        /** @brief Return an empty permuter with size 0.

          Elements will be allocated in order 0, 1, ..., @a width - 1. */
        static inline uint64_t make_empty() {
            uint64_t p = (uint64_t) INITIAL_VALUE;
            return p & ~(uint64_t) (LEAF_WIDTH);
        }   /* make an empty permutation */

        /** @brief Return a permuter with size @a n.

          The returned permutation has size() @a n. For 0 <= i < @a n,
          (*this)[i] == i. Elements n through @a width - 1 are free, and will be
          allocated in that order. */
        static inline uint64_t make_sorted(int n) {
            uint64_t mask = (n == LEAF_WIDTH ? (uint64_t) 0 : (uint64_t) 16 << (n << 2)) - 1;
            return (make_empty() << (n << 2))
                | ((uint64_t) FULL_VALUE & mask)
                | n;
        }   /* make a permuation of length n with already sorted elements */

        /** @brief Return the permuter's size. */
        int size() const {
            return x_ & LEAF_WIDTH;
        }

        /** @brief Return the permuter's element @a i.
          @pre 0 <= i < width */
        int operator[](int i) const {
            return (x_ >> ((i << 2) + 4)) & LEAF_WIDTH;
        }   

        int back() const {
            return (*this)[LEAF_WIDTH - 1];
        }   /* the back element which is always free if not full */

        uint64_t value() const {
            return x_;
        }   /* the raw permutation */

        uint64_t value_from(int i) const {
            return x_ >> ((i + 1) << 2);
        }   /* the permutation starting from ith element */

        void set_size(int n) {
            x_ = (x_ & ~(uint64_t)LEAF_WIDTH) | n;
        }   /* changing the num of elements */

        /** @brief Allocate a new element and insert it at position @a i.
          @pre 0 <= @a i < @a width
          @pre size() < @a width
          @return The newly allocated element.

          Consider the following code:
          <code>
          kpermuter<...> p = ..., q = p;
          int x = q.insert_from_back(i);
          </code>

          The modified permuter, q, has the following properties.
          <ul>
          <li>q.size() == p.size() + 1</li>
          <li>Given j with 0 <= j < i, q[j] == p[j] && q[j] != x</li>
          <li>Given j with j == i, q[j] == x</li>
          <li>Given j with i < j < q.size(), q[j] == p[j-1] && q[j] != x</li>
          </ul> */
        int insert_from_back(int i) {
            int value = back();
            // increment size, leave lower slots unchanged
            x_ = ((x_ + 1) & (((uint64_t) 16 << (i << 2)) - 1))
                // insert slot
                | ((uint64_t) value << ((i << 2) + 4))
                // shift up unchanged higher entries & empty slots
                | ((x_ << 4) & ~(((uint64_t) 256 << (i << 2)) - 1));
            return value;
        }

        /** @brief Insert an unallocated element from position @a si at position @a di.
          @pre 0 <= @a di < @a width
          @pre size() < @a width
          @pre size() <= @a si
          @return The newly allocated element. */
        void insert_selected(int di, int si) {
            int value = (*this)[si];
            uint64_t mask = ((uint64_t) 256 << (si << 2)) - 1;
            // increment size, leave lower slots unchanged
            x_ = ((x_ + 1) & (((uint64_t) 16 << (di << 2)) - 1))
                // insert slot
                | ((uint64_t) value << ((di << 2) + 4))
                // shift up unchanged higher entries & empty slots
                | ((x_ << 4) & mask & ~(((uint64_t) 256 << (di << 2)) - 1))
                // leave uppermost slots alone
                | (x_ & ~mask);
        }
        /** @brief Remove the element at position @a i.
          @pre 0 <= @a i < @a size()
          @pre size() < @a width

          Consider the following code:
          <code>
          kpermuter<...> p = ..., q = p;
          q.remove(i);
          </code>

          The modified permuter, q, has the following properties.
          <ul>
          <li>q.size() == p.size() - 1</li>
          <li>Given j with 0 <= j < i, q[j] == p[j]</li>
          <li>Given j with i <= j < q.size(), q[j] == p[j+1]</li>
          <li>q[q.size()] == p[i]</li>
          </ul> */
        void remove(int i) {
            if (int(x_ & 15) == i + 1)
                --x_;
            else {
                int rot_amount = ((x_ & 15) - i - 1) << 2;
                uint64_t rot_mask =
                    (((uint64_t) 16 << rot_amount) - 1) << ((i + 1) << 2);
                // decrement size, leave lower slots unchanged
                x_ = ((x_ - 1) & ~rot_mask)
                    // shift higher entries down
                    | (((x_ & rot_mask) >> 4) & rot_mask)
                    // shift value up
                    | (((x_ & rot_mask) << rot_amount) & rot_mask);
            }
        }
        /** @brief Remove the element at position @a i to the back.
          @pre 0 <= @a i < @a size()
          @pre size() < @a width

          Consider the following code:
          <code>
          kpermuter<...> p = ..., q = p;
          q.remove_to_back(i);
          </code>

          The modified permuter, q, has the following properties.
          <ul>
          <li>q.size() == p.size() - 1</li>
          <li>Given j with 0 <= j < i, q[j] == p[j]</li>
          <li>Given j with i <= j < @a width - 1, q[j] == p[j+1]</li>
          <li>q.back() == p[i]</li>
          </ul> */
        void remove_to_back(int i) {
            uint64_t mask = ~(((uint64_t) 16 << (i << 2)) - 1);
            // clear unused slots
            uint64_t x = x_ & (((uint64_t) 16 << (LEAF_WIDTH << 2)) - 1);
            // decrement size, leave lower slots unchanged
            x_ = ((x - 1) & ~mask)
                // shift higher entries down
                | ((x >> 4) & mask)
                // shift removed element up
                | ((x & mask) << ((LEAF_WIDTH - i - 1) << 2));
        }
        /** @brief Rotate the permuter's elements between @a i and size().
          @pre 0 <= @a i <= @a j <= size()

          Consider the following code:
          <code>
          kpermuter<...> p = ..., q = p;
          q.rotate(i, j);
          </code>

          The modified permuter, q, has the following properties.
          <ul>
          <li>q.size() == p.size()</li>
          <li>Given k with 0 <= k < i, q[k] == p[k]</li>
          <li>Given k with i <= k < q.size(), q[k] == p[i + (k - i + j - i) mod (size() - i)]</li>
          </ul> */
        void rotate(int i, int j) {
            uint64_t mask = (i == LEAF_WIDTH ? (uint64_t) 0 : (uint64_t) 16 << (i << 2)) - 1;
            // clear unused slots
            uint64_t x = x_ & (((uint64_t) 16 << (LEAF_WIDTH << 2)) - 1);
            x_ = (x & mask)
                | ((x >> ((j - i) << 2)) & ~mask)
                | ((x & ~mask) << ((LEAF_WIDTH - j) << 2));
        }
        /** @brief Exchange the elements at positions @a i and @a j. */
        void exchange(int i, int j) {
            uint64_t diff = ((x_ >> (i << 2)) ^ (x_ >> (j << 2))) & 240;
            x_ ^= (diff << (i << 2)) | (diff << (j << 2));
        }
        /** @brief Exchange positions of values @a x and @a y. */
        void exchange_values(int x, int y) {
            uint64_t diff = 0, p = x_;
            for (int i = 0; i < LEAF_WIDTH; ++i, diff <<= 4, p <<= 4) {
                int v = (p >> (LEAF_WIDTH << 2)) & 15;
                diff ^= -((v == x) | (v == y)) & (x ^ y);
            }
            x_ ^= diff;
        }

        bool operator==(const permuter& x) const {
            return x_ == x.x_;
        }
        bool operator!=(const permuter& x) const {
            return !(*this == x);
        }

        int operator&(uint64_t mask) {
            return x_ & mask;
        }

        void operator>>=(uint64_t mask) {
            x_ = (x_ >> mask);
        }

        static inline int size(uint64_t p) {
            return p & 15;
        }

    private:
        uint64_t x_;
};

typedef struct key_indexed_position 
{
    int i; /* the position in the sorted order */
    int p; /* the actual position of the key */
    inline key_indexed_position() {
    }
    inline constexpr key_indexed_position(int i_, int p_)
        : i(i_), p(p_) {
    }
} key_indexed_position;

class kv
{
    private:
        u_int64_t   key;
        void        *link_or_value;
    
    public:

        kv():link_or_value(NULL){}
        kv(u_int64_t key, void *value):key(key),link_or_value(value){}

        friend class inner_node;
        friend class leaf_node;
        friend class btree;
};

class inner_node
{
    private:
        inner_node              *parent,                            //8B
                                *right,                             //8B
                                *left;                              //8B
        VersionNumber           version;                            //8B
        u_int64_t               highkey;                            //8B
        u_int64_t               lowkey;                             //8B
        permuter                permutation;                        //8B
        void                    *child0;                            //8B
        kv                      entry[LEAF_WIDTH];                  //240B

    public:

        inner_node():
            parent(NULL),
            right(NULL),
            left(NULL),
            child0(NULL),
            highkey(UINT64_MAX),
            lowkey(0),
            permutation(permuter::make_empty())
            {}

        inner_node(void *parent, void *right, void *left):
            parent(reinterpret_cast<inner_node *>(parent)),
            right(reinterpret_cast<inner_node *>(right)),
            left(reinterpret_cast<inner_node *>(left)),
            child0(NULL),
            highkey(UINT64_MAX),
            lowkey(0),
            permutation(permuter::make_empty())
            {}
        
        inner_node(const inner_node* node)
        {
            *this = *node;
        }

        void *operator new(size_t size);
        void operator delete(void *addr);

        int compare_key(const uint64_t a, const uint64_t b);
        key_indexed_position key_lower_bound_by(uint64_t key);
        key_indexed_position key_lower_bound(uint64_t key);

        int size();
        void insert(u_int64_t key, void* value);
        kv split(u_int64_t key, void* value, VersionNumber* &v1, VersionNumber* &v2);
        int rebalance(u_int64_t key, void* value, VersionNumber* &v1, VersionNumber* &v2);
        int remove(u_int64_t key);
        inner_node* give_parent();
        void del();
        void* get(u_int64_t key);
        void* get_exact(u_int64_t key);

        int full(){return permutation.size()==LEAF_WIDTH;}
        int empty(){return child0==NULL;}
        int capacity(){return LEAF_WIDTH+1;}

        friend class btree;
        friend class leaf_node;
        
};

class leaf_node
{
    private:
        inner_node              *parent;                            //8B
        leaf_node               *right,                             //8B
                                *left;                              //8B
        VersionNumber           version;                            //8B
        u_int64_t               highkey,                            //8B
                                lowkey;                             //8B
        permuter                permutation;                        //8B
        u_int64_t               dummy;                              //8B
        kv                      entry[LEAF_WIDTH];                  //240B
    
    public:

        leaf_node():
            parent(NULL),
            right(NULL),
            left(NULL),
            highkey(UINT64_MAX),
            lowkey(0),
            permutation(permuter::make_empty())
            {
                version.markLeaf();
            }

        leaf_node(void *parent, void *right, void *left):
            parent(reinterpret_cast<inner_node *>(parent)),
            right(reinterpret_cast<leaf_node *>(right)),
            left(reinterpret_cast<leaf_node *>(left)),
            highkey(UINT64_MAX),
            lowkey(0),
            permutation(permuter::make_empty())
            {
                version.markLeaf();
            }

        leaf_node(const leaf_node* node) 
        {
            *this = *node;
        }

        void *operator new(size_t size);
        void operator delete(void *addr);

        int compare_key(const uint64_t a, const uint64_t b);
        key_indexed_position key_lower_bound_by(uint64_t key);
        key_indexed_position key_lower_bound(uint64_t key);

        int size();
        void insert(u_int64_t key, void* value);
        kv split(u_int64_t key, void* value, VersionNumber* &v1, VersionNumber* &v2);
        int rebalance(u_int64_t key, void* value);
        int remove(u_int64_t key);
        inner_node* give_parent();
        void del();
        void* get(u_int64_t key);

        int full(){return permutation.size()==LEAF_WIDTH;}
        int empty(){return permutation.size()==0;}
        int capacity(){return LEAF_WIDTH;}

        friend class btree;
        friend class inner_node;
};

class btree
{
    private:
        void *root_;

    public:
        btree(){init_root();}
        btree(void *root):root_(root){}
        void *operator new(size_t size);
        void operator delete(void *addr);

        int insert(u_int64_t key, void *value);
        void remove(u_int64_t key);
        void* get(u_int64_t key);

        u_int64_t tot_nodes();
        double efficiency();
        u_int64_t tot_lookups();
        u_int64_t tot_inserts();
        u_int64_t tot_rebalances();

    private:
        void new_root();
        VersionNumber get_version(void *node);
        void* get_child0(void *node)
        {
            return node+56;
        }
        void init_root();
};

}