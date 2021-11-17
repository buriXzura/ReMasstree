#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <math.h>
#include <iostream>
#include <mutex>
#include <atomic>
#include <assert.h>
#include <emmintrin.h>
#include "masstree.h"

static inline void fence() {
    asm volatile("" : : : "memory");
}

static inline void mfence() {
    asm volatile("sfence":::"memory");
}

static inline long long atomic_or(long long *object, long long addend) {
    asm volatile("lock; orb %0,%1"
                 : "=r" (addend), "+m" (*object) : : "cc");
    fence();
    return addend;
}

static inline long long xchg(long long *object, long long new_value) {
        asm volatile("xchgb %0,%1"
            : "+q" (new_value), "+m" (*object));
    fence();
    return new_value;
}

class Node {
public:
    Node* parent;
    Node* right;
    Node* left;
    uint64_t highkey;
    uint64_t lowkey;
    masstree::VersionNumber version;
    masstree::permuter permutation;
    masstree::kv entry[12];
};

class Thread {
public:
    uint64_t Key;
    void* value;
} thread;

void transit_parent_to_child(Node* p, Node* c) {
    retry_from_root:

    /*
    Start traversal
    */

    retry_from_parent:

    /*
    Operations/Lookups on parent's node
    */

    masstree::VersionNumber V1=p->version, V2=c->version;

    //Check the parent's version for consistency
    if(V1!=p->version) {
        if(V1.smoVersion()!=p->version.smoVersion()) {
            if(p->lowkey<=thread.Key&&thread.Key<p->highkey)
                goto retry_from_parent;
            else 
                //check siblings and then give-up
                goto retry_from_root;
        }
        else if(V1.insertVersion()!=p->version.insertVersion()) {
            if(p->lowkey<=thread.Key&&thread.Key<p->highkey)
                goto retry_from_parent;
            else 
                goto retry_from_siblings;
        }
        else
            goto retry_from_parent;
    }
    else {
        V1 = V2;
        goto retry_from_child;
    }


    retry_from_siblings:


    retry_from_child:
        
        if(c->version.insertLock()) {
            while(c->version.insertLock());
            if(V1.smoVersion()!=c->version.smoVersion()) {
                if(c->lowkey<=thread.Key&&thread.Key<c->highkey)
                    V1 = c->version;
                else 
                    goto retry_from_root;
            }
            else if(c->version.smoLock()) {
                if(p->lowkey<=thread.Key&&thread.Key<p->highkey)
                    V1 = c->version;
                else 
                    goto retry_from_siblings;
            }
            else 
                V1 = c->version;
        }
        else 
            goto retry_from_parent;

}

void transit_to_right_sibling(Node *a, Node *b) {
    retry_from_root:

    if(thread.Key>=a->highkey)
        goto try_from_right;

    try_from_right:

    if (b->lowkey<=thread.Key&&thread.Key<b->highkey)
        goto proceed;
    else 
        goto retry_from_root;

    proceed:

}

int main() {
    masstree::VersionNumber version;

}