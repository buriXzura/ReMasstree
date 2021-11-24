#include "masstree.h"

#ifdef DRAM
#define RRP_free free 
#define RRP_malloc malloc
#else
#define RRP_free RP_free
#define RRP_malloc RP_malloc
#endif

namespace masstree
{
    static constexpr uint64_t CACHE_LINE_SIZE = 64;

    static inline void fence() {
        asm volatile("" : : : "memory");
    }

    static inline void mfence() {
        asm volatile("sfence":::"memory");
    }

    static inline void clflush(char *data, int len, bool front, bool back)
    {
        volatile char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
        if (front)
            mfence();
        for(; ptr<data+len; ptr+=CACHE_LINE_SIZE){
    #ifdef CLFLUSH
            asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
    #elif CLFLUSH_OPT
            asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
    #elif CLWB
            asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
    #endif
        }
        if (back)
            mfence();
    }

    static inline void movnt64(uint64_t *dest, uint64_t const &src, bool front, bool back) {
        assert(((uint64_t)dest & 7) == 0);
        if (front) mfence();
        _mm_stream_si64((long long int *)dest, *(long long int *)&src);
        if (back) mfence();
    }

    static inline void prefetch_(const void *ptr)
    {
        typedef struct { char x[CACHE_LINE_SIZE]; } cacheline_t;
        asm volatile("prefetcht0 %0" : : "m" (*(const cacheline_t *)ptr));
    }

    uint64_t lock_version=100;
    VersionNumber::VersionNumber():
    v(lock_version<<44) 
    {}

    bool VersionNumber::repair_req()
    {
        return (smoLock()||insertLock())&&(lockVersion()!=lock_version);
    }

    uint VersionNumber::updateLock() {
        volatile uint64_t current = *(volatile uint64_t*)&v;
        uint temp = (current&LOCK_VERSION)>>44;
        if(temp==lock_version)
            return temp;
        return (__sync_val_compare_and_swap(&v,current,(current&LOCK_RESET)|(lock_version<<44)|INSERT_LOCK) & LOCK_VERSION)>>44;
    }

    void update_parent(void* node, inner_node* parent) 
    {
        inner_node **value_par;
        value_par = reinterpret_cast<inner_node **>(node);
        *value_par=parent;
    }

    u_int64_t   num_nodes=0;
    double      space=0;
    u_int64_t   num_inserts=0;
    u_int64_t   num_rebalances=0;
    u_int64_t   num_lookups=0;

    int inner_node::size()
    {
        int size=0;
        if(child0)
            size++;
        size+=permutation.size();
        return size;
    }

    int leaf_node::size()
    {
        return permutation.size();
    }

    int leaf_node::compare_key(const uint64_t a, const uint64_t b)
    {
        if (a == b)
            return 0;
        else
            return a < b ? -1 : 1;
    }

    int inner_node::compare_key(const uint64_t a, const uint64_t b)
    {
        if (a == b)
            return 0;
        else
            return a < b ? -1 : 1;
    }

    key_indexed_position leaf_node::key_lower_bound_by(uint64_t key)
    {
        permuter perm = permutation;
        int l = 0, r = perm.size();
        while (l < r) {
            int m = (l + r) >> 1;
            int mp = perm[m];
            int cmp = compare_key(key, entry[mp].key);
            if (cmp < 0)
                r = m;
            else if (cmp == 0)
                return key_indexed_position(m, mp);
            else
                l = m + 1;
        }
        return l < LEAF_WIDTH ? key_indexed_position(l,perm[l]) : key_indexed_position(l,-1);
    }

    key_indexed_position leaf_node::key_lower_bound(uint64_t key)
    {
        permuter perm = permutation;
        int l = 0, r = perm.size();
        while (l < r) {
            int m = (l + r) >> 1;
            int mp = perm[m];
            int cmp = compare_key(key, entry[mp].key);
            if (cmp < 0)
                r = m;
            else if (cmp == 0)
                return key_indexed_position(m, mp);
            else
                l = m + 1;
        }

        return (l-1 < 0 ? key_indexed_position(l-1, -1) : key_indexed_position(l-1, perm[l-1]));
    }

    key_indexed_position inner_node::key_lower_bound_by(uint64_t key)
    {
        permuter perm = permutation;
        int l = 0, r = perm.size();
        while (l < r) {
            int m = (l + r) >> 1;
            int mp = perm[m];
            int cmp = compare_key(key, entry[mp].key);
            if (cmp < 0)
                r = m;
            else if (cmp == 0)
                return key_indexed_position(m, mp);
            else
                l = m + 1;
        }
        return l < LEAF_WIDTH ? key_indexed_position(l,perm[l]) : key_indexed_position(l,-1);
    }

    key_indexed_position inner_node::key_lower_bound(uint64_t key)
    {
        permuter perm = permutation;
        int l = 0, r = perm.size();
        while (l < r) {
            int m = (l + r) >> 1;
            int mp = perm[m];
            int cmp = compare_key(key, entry[mp].key);
            if (cmp < 0)
                r = m;
            else if (cmp == 0)
                return key_indexed_position(m, mp);
            else
                l = m + 1;
        }

        return (l-1 < 0 ? key_indexed_position(l-1, -1) : key_indexed_position(l-1, perm[l-1]));
    }

    void leaf_node::insert(uint64_t key, void *value)
    {
        permuter temp = permutation.value();
        key_indexed_position ip = key_lower_bound_by(key);
        if(ip.i==LEAF_WIDTH)
            return;
        int pos=temp.insert_from_back(ip.i);

        entry[pos].link_or_value=value;
        entry[pos].key=key;

        permutation = temp.value();

#ifdef STATS
        space*=num_nodes;
        space+=((double)1)/capacity();
        space/=num_nodes;
        num_inserts++;
#endif

    }

    void inner_node::insert(uint64_t key, void *value)
    {
        permuter temp=permutation.value();
        key_indexed_position ip = key_lower_bound_by(key);
        if(ip.i==LEAF_WIDTH)
            return;
        int pos=temp.insert_from_back(ip.i);

        entry[pos].link_or_value=value;
        entry[pos].key=key;

        permutation = temp.value();

        update_parent(value, this);

#ifdef STATS
        space*=num_nodes;
        space+=((double)1)/capacity();
        space/=num_nodes;
        num_inserts++;
#endif

    }


    inner_node* leaf_node::give_parent()
    {
        return parent;
    }

    inner_node* inner_node::give_parent()
    {
        return parent;
    }

    void* leaf_node::get(u_int64_t key)
    {
        key_indexed_position ip=key_lower_bound(key);
        if(ip.i<0)
            return NULL;
        if(compare_key(entry[ip.p].key,key)==0)
            return entry[ip.p].link_or_value;
        return NULL;
    }

    void* inner_node::get(u_int64_t key)
    {
        key_indexed_position ip=key_lower_bound(key);  
        if(ip.i<0)
            return child0;
        return entry[ip.p].link_or_value; 
    }

    void* inner_node::get_exact(u_int64_t key)
    {
        key_indexed_position ip = key_lower_bound_by(key);
        if(ip.i==permutation.size())
        {
            if(compare_key(key,highkey)==0)
                return entry[permutation[ip.i-1]].link_or_value;
            return NULL;
        }
        if(compare_key(key,entry[ip.p].key)!=0)
            return NULL;
        if(ip.i==0)
            return child0;
        return entry[permutation[ip.i-1]].link_or_value;
    }

    kv leaf_node::split(u_int64_t key, void *value, VersionNumber* &v1, VersionNumber* &v2)
    {   
        //std::cout<<"leaf split\n";
        int mid=size()+1;
        mid/=2;

        permuter temp=permutation.value();

        leaf_node *nr;
        nr = new leaf_node(parent,right,this);

        permuter nper = temp.value();
        nper.rotate(0,mid);
        nper.set_size(size()-mid);
        nr->permutation=nper.value();
        nr->highkey=highkey;
        nr->lowkey=entry[temp[mid]].key;

        for(int i=mid; i<size(); i++)
            nr->entry[temp[i]]=entry[temp[i]];
        
        nr->version.trySMOLock();
        
        right=nr;
        highkey=entry[temp[mid]].key;
        if(nr->right)
        {
            nr->right->left=nr;
        }
        permutation.set_size(mid);

        if(compare_key(key,highkey)<0)
            insert(key,value);
        else 
            nr->insert(key,value);
        
        v1=&version;
        v2=&nr->version;

        v1->incrementInsert();
        v2->incrementInsert();
        v1->releaseInsertLock();
        v2->releaseInsertLock();

        return kv(highkey,reinterpret_cast<void *>(nr));
    }

    kv inner_node::split(u_int64_t key, void* value, VersionNumber* &v1, VersionNumber* &v2)
    {
        //std::cout<<"inner split\n";
        int mid=size()+1;
        mid/=2;

        permuter temp=permutation.value();

        inner_node *nr;
        nr = new inner_node(parent,right,this);

        permuter nper = temp.value();
        nper.rotate(0,mid);
        nper.set_size(temp.size()-mid);
        nr->permutation=nper.value();
        nr->highkey=highkey;
        nr->lowkey=entry[temp[mid-1]].key;

        nr->child0=entry[temp[mid-1]].link_or_value;
        for(int i=mid; i<temp.size(); i++)
            nr->entry[temp[i]]=entry[temp[i]];

        nr->version.trySMOLock();

        right=nr;
        highkey=entry[temp[mid-1]].key;
        if(nr->right)
        {
            nr->right->left=nr;
        }
        permutation.set_size(mid-1);

        for(int i=mid-1; i<temp.size(); i++)
        {
            inner_node **value_par;
            value_par = reinterpret_cast<inner_node **>(entry[temp[i]].link_or_value);
            *value_par = nr;
        }

        int cmp=compare_key(key,highkey);
        if(cmp<0)
            insert(key,value);
        else if(cmp>0)
            nr->insert(key,value);
        
        v1->releaseSMOLock();
        v2->releaseSMOLock();
        
        v1=&version;
        v2=&nr->version;

        v1->incrementInsert();
        v2->incrementInsert();
        v1->releaseInsertLock();
        v2->releaseInsertLock();
        
        return kv(highkey,reinterpret_cast<void *>(nr));
    }

    int leaf_node::rebalance(u_int64_t key, void* value)
    {
        //std::cout<<"leaf rebalance\n";
        #ifndef REBAL
        return 0;
        #endif

        key_indexed_position ip = key_lower_bound_by(key);
        int mx_sze=LEAF_WIDTH;
        int to_mov;
        leaf_node *temp_l;
        inner_node *par;
        
        left_sibing:
            //goto right_sibling;
            if(left==NULL)
                goto right_sibling;
            
            while(1) {
                temp_l=left;
                while(temp_l->version.tryInsertLock()) {
                    if( (temp_l->parent!=parent) || (temp_l->full()) || (temp_l->version.smoLock()) )
                        goto right_sibling;
                }
                if(temp_l==left)
                    break;
                temp_l->version.releaseInsertLock();
            }
            if( (left->parent!=parent) || (left->full()) ) {
                left->version.releaseInsertLock();
                goto right_sibling;
            }
            while(1) {
                par=parent;
                while(par->version.tryInsertLock());
                if(par==parent)
                    break;
                par->version.releaseInsertLock();
            }
            if(left->parent!=parent) {
                left->version.releaseInsertLock();
                parent->version.releaseInsertLock();
                goto right_sibling;
            }

            to_mov = mx_sze-left->size();
            to_mov /= (1+(ip.i<mx_sze));
            to_mov = to_mov > 1 ? to_mov : 1;

            if(left->size()+to_mov<=mx_sze && to_mov<=ip.i)
            {
                while(left->version.trySMOLock());

                int base=left->size();
                permuter temp = left->permutation.value();
                for(int i=0; i<to_mov; i++)
                {
                    left->entry[temp[i+base]]=entry[permutation[i]];
                }
                temp.set_size(base+to_mov);
                left->permutation=temp.value();

                key_indexed_position p_upd = parent->key_lower_bound_by(left->highkey);

                if(to_mov==ip.i)
                {
                    parent->entry[p_upd.p].key=key;

                    temp = permutation.value();
                    temp.rotate(0,to_mov);
                    temp.set_size(temp.size()-to_mov);
                    permutation=temp.value();

                    int pos=temp.insert_from_back(0);
                    entry[pos].key=key;
                    entry[pos].link_or_value=value;
                    permutation=temp.value();

                    left->highkey=key;
                    lowkey=key;

#ifdef STATS
                    space*=num_nodes;
                    space+=((double)1)/capacity();
                    space/=num_nodes;
#endif
                } else {
                    parent->entry[p_upd.p].key=entry[permutation[to_mov]].key;

                    temp = permutation.value();
                    temp.rotate(0,to_mov);
                    temp.set_size(temp.size()-to_mov);
                    permutation=temp.value();

                    left->highkey=entry[temp[0]].key;
                    lowkey=entry[temp[0]].key;
                    insert(key,value);
                }

                left->version.releaseBothLocks();
                version.releaseBothLocks();
                parent->version.incrementInsert();
                parent->version.releaseInsertLock();

                return 1;
            }
            left->version.releaseInsertLock();
            parent->version.releaseInsertLock();

        right_sibling:

            if(right==NULL)
                goto end;
            
            while(1) {
                temp_l=right;
                while(temp_l->version.tryInsertLock()) {
                    if( (temp_l->parent!=parent) || (temp_l->full()) || (temp_l->version.smoLock()) )
                        goto end;
                }
                if(temp_l==right)
                    break;
                temp_l->version.releaseInsertLock();
            }
            if( (right->parent!=parent) || (right->full()) ) {
                right->version.releaseInsertLock();
                goto end;
            }
            while(1) {
                par=parent;
                while(par->version.tryInsertLock());
                if(par==parent)
                    break;
                par->version.releaseInsertLock();
            }
            if(right->parent!=parent) {
                right->version.releaseInsertLock();
                parent->version.releaseInsertLock();
                goto end;
            }
            
            to_mov = mx_sze-right->size();
            to_mov /= (1+(ip.i>0));
            to_mov = to_mov > 1 ? to_mov : 1;

            if(right->size()+to_mov<=mx_sze && to_mov<=mx_sze-ip.i)
            {
                while(right->version.trySMOLock());

                int base=right->size();
                permuter temp = right->permutation.value();
                temp.rotate(0,mx_sze-to_mov);
                temp.set_size(base+to_mov);
                for(int i=0; i<to_mov; i++)
                {
                    right->entry[temp[i]]=entry[permutation[mx_sze-to_mov+i]];
                }
                right->permutation=temp.value();

                key_indexed_position p_upd = parent->key_lower_bound_by(highkey);
                parent->entry[p_upd.p].key=right->entry[temp[0]].key;

                permutation.set_size(mx_sze-to_mov);

                highkey=right->entry[temp[0]].key;
                right->lowkey=right->entry[temp[0]].key;

                insert(key,value);
                
                right->version.releaseBothLocks();
                version.releaseBothLocks();
                parent->version.incrementInsert();
                parent->version.releaseInsertLock();

                return 1;
            }
            right->version.releaseInsertLock();
            parent->version.releaseInsertLock();

        end:
            return 0;
    }

    int inner_node::rebalance(u_int64_t key, void* value, VersionNumber* &v1, VersionNumber* &v2)
    {
        //std::cout<<"inner rebalance\n";
        #ifndef REBAL
        return 0;
        #endif

        key_indexed_position ip = key_lower_bound_by(key);
        int mx_sze=LEAF_WIDTH+1;
        int to_mov;
        int added=0;
        inner_node *temp_i, *par;

        left_sibling:
            if(left==NULL)
                goto right_sibling;
            
            while(1) {
                temp_i=left;
                while(temp_i->version.tryInsertLock()) {
                    if( (temp_i->parent!=parent) || (temp_i->full()) || (temp_i->version.smoLock()) )
                        goto right_sibling;
                }
                if(temp_i==left)
                    break;
                temp_i->version.releaseInsertLock();
            }
            if( (left->parent!=parent) || (left->full()) ) {
                left->version.releaseInsertLock();
                goto right_sibling;
            }
            while(1) {
                par=parent;
                while(par->version.tryInsertLock());
                if(par==parent)
                    break;
                par->version.releaseInsertLock();
            }
            if(left->parent!=parent) {
                left->version.releaseInsertLock();
                parent->version.releaseInsertLock();
                goto right_sibling;
            }

            to_mov=mx_sze-left->size();
            to_mov/=(1+(ip.i<mx_sze-1));
            to_mov = to_mov > 1 ? to_mov : 1;

            if(left->size()+to_mov<=mx_sze && to_mov<=ip.i+1)
            {
                while(left->version.trySMOLock());

                permuter temp = left->permutation.value();
                int base=temp.size();
                key_indexed_position p_upd = parent->key_lower_bound_by(left->highkey);
                
                temp.set_size(base+to_mov);
                for(int i=1; i<to_mov; i++)
                {
                    left->entry[temp[base+i]]=entry[permutation[i-1]];
                }
                left->entry[temp[base]].key=parent->entry[p_upd.p].key;
                left->entry[temp[base]].link_or_value=child0;
                left->permutation=temp.value();

                if(ip.i+1==to_mov)
                {
                    parent->entry[p_upd.p].key=key;

                    child0=value;
                    {
                        update_parent(child0, this);
                    }
                    temp = permutation.value();
                    temp.set_size(temp.size()-to_mov+1);
                    temp.rotate(0,to_mov-1);
                    permutation = temp.value();

                    v1->releaseSMOLock();
                    v2->releaseSMOLock();

                    temp=left->permutation.value();
                    for(int i=base; i<base+to_mov; i++)
                    {
                        update_parent(left->entry[temp[i]].link_or_value, left);
                    }

                    left->highkey=parent->entry[p_upd.p].key;
                    lowkey=parent->entry[p_upd.p].key;

#ifdef STATS
                    space*=num_nodes;
                    space+=((double)1)/capacity();
                    space/=num_nodes;
#endif

                } else 
                {
                    temp = permutation.value();
                    parent->entry[p_upd.p].key=entry[temp[to_mov-1]].key;

                    child0=entry[temp[to_mov-1]].link_or_value;
                    temp.set_size(temp.size()-to_mov);
                    temp.rotate(0,to_mov);
                    permutation = temp.value();

                    temp = left->permutation.value();
                    for(int i=base; i<base+to_mov; i++)
                    {
                        update_parent(left->entry[temp[i]].link_or_value, left);
                    }

                    left->highkey=parent->entry[p_upd.p].key;
                    lowkey=parent->entry[p_upd.p].key;
                    insert(key,value);

                    v1->releaseSMOLock();
                    v2->releaseSMOLock();
                }

                left->version.releaseBothLocks();
                version.releaseBothLocks();
                parent->version.incrementInsert();
                parent->version.releaseInsertLock();

                return 1;
            }
            left->version.releaseInsertLock();
            parent->version.releaseInsertLock();
                                                                           
        right_sibling:
            if(right==NULL)
                goto end;

            while(1) {
                temp_i=right;
                while(temp_i->version.tryInsertLock()) {
                    if( (temp_i->parent!=parent) || (temp_i->full()) || (temp_i->version.smoLock()) )
                        goto end;
                }
                if(temp_i==right)
                    break;
                temp_i->version.releaseInsertLock();
            }
            if( (right->parent!=parent) || (right->full()) ) {
                right->version.releaseInsertLock();
                goto end;
            }
            while(1) {
                par=parent;
                while(par->version.tryInsertLock());
                if(par==parent)
                    break;
                par->version.releaseInsertLock();
            }
            if(right->parent!=parent) {
                right->version.releaseInsertLock();
                parent->version.releaseInsertLock();
                goto end;
            }
            
            to_mov=mx_sze-right->size();
            to_mov/=2;
            to_mov = to_mov > 1 ? to_mov : 1;

            if(right->size()+to_mov<=mx_sze && to_mov<mx_sze-ip.i)
            {
                while(right->version.trySMOLock());

                int base=right->size();
                permuter temp = right->permutation.value();
                temp.rotate(0,LEAF_WIDTH-to_mov);
                temp.set_size(temp.size()+to_mov);
                for(int i=0; i<to_mov-1; i++)
                {
                    right->entry[temp[i]]=entry[permutation[mx_sze-to_mov+i]];
                }
                right->entry[temp[to_mov-1]].key=highkey;
                right->entry[temp[to_mov-1]].link_or_value=right->child0;
                right->permutation=temp.value();
                right->child0=entry[permutation[LEAF_WIDTH-to_mov]].link_or_value;

                key_indexed_position p_upd = parent->key_lower_bound_by(highkey);
                parent->entry[p_upd.p].key = entry[permutation[LEAF_WIDTH-to_mov]].key;

                permutation.set_size(LEAF_WIDTH-to_mov);

                for(int i=LEAF_WIDTH-to_mov; i<LEAF_WIDTH; i++)
                {
                    update_parent(entry[permutation[i]].link_or_value, right);
                }

                highkey=parent->entry[p_upd.p].key;
                right->lowkey=parent->entry[p_upd.p].key;
                insert(key,value);

                v1->releaseSMOLock();
                v2->releaseSMOLock();

                right->version.releaseBothLocks();
                version.releaseBothLocks();
                parent->version.incrementInsert();
                parent->version.releaseInsertLock();

                return 1;
            }
            right->version.releaseInsertLock();
            parent->version.releaseInsertLock();

        end:
            return 0;
    }

    void* btree::get(u_int64_t key)
    {
        void *p;
        inner_node *inner, *temp_i;
        leaf_node *leaf, *temp_l;
        VersionNumber V1, V2;
        key_indexed_position ip;
        bool comp;

        from_root:
            p=root_;
            V1=get_version(p);

            if(V1.isLeaf())
                goto from_leaf;

        from_inner:
            inner = reinterpret_cast<inner_node *>(p);
            p = inner->get(key);

            V2 = get_version(p);
            if( (V1!=inner->version) || (inner->version.insertLock()) ) {
                if(V1.isRoot())
                    goto from_root;
                V2=inner->version;
                if( (V2.smoVersion()!=V1.smoVersion()) || (V2.smoLock()) ) {
                    while(1) {
                        temp_i=inner->right;
                        comp=temp_i;
                        if(comp) {
                            V1=temp_i->version;
                            comp=(key>=inner->highkey);
                        }
                        if(temp_i==inner->right)
                            break;
                    }
                    if(comp) {
                        inner=temp_i;
                        if(key<inner->highkey) {
                            p=inner;
                            goto from_inner;
                        }
                        else {
                            goto from_root;
                        }
                    } 
                    else {
                        while(1) {
                            temp_i=inner->left;
                            comp=temp_i;
                            if(comp) {
                                V1=temp_i->version;
                                comp=(key<temp_i->highkey);
                            }
                            if(temp_i==inner->left)
                                break;
                        }
                        if(comp) {
                            inner=temp_i;
                            ip = inner->key_lower_bound(key);
                            if(ip.i<0)
                                goto from_root;
                            p=inner;
                            goto from_inner;
                        }
                        else {
                            V1=V2;
                            p=inner;
                            goto from_inner;
                        }
                    }
                }
                V1=V2;
                goto from_inner;
            }
            V1=V2;

            if(p==NULL)
                return NULL;
            if( V1.isLeaf() )
                goto from_leaf;
            goto from_inner;

        from_leaf:
            leaf = reinterpret_cast<leaf_node *>(p);
            p = leaf->get(key);
            
            if( (V1!=leaf->version) || (leaf->version.insertLock()) ) {
                if(V1.isRoot())
                    goto from_root;
                V2=leaf->version;
                if( (V2.smoVersion()!=V1.smoVersion()) || (V2.smoLock()) ) {
                    while(1) {
                        temp_l=leaf->right;
                        comp=temp_l;
                        if(comp) {
                            V1=temp_l->version;
                            comp=(key>=leaf->highkey);
                        }
                        if(temp_l==leaf->right)
                            break;
                    }
                    if(comp) {
                        leaf=temp_l;
                        if(key<leaf->highkey) {
                            p=leaf;
                            goto from_leaf;
                        }
                        else {
                            goto from_root;
                        }
                    } 
                    else {
                        while(1) {
                            temp_l=leaf->left;
                            comp=temp_l;
                            if(comp) {
                                V1=temp_l->version;
                                comp=(key<temp_l->highkey);
                            }
                            if(temp_l==leaf->left)
                                break;
                        }
                        if(comp) {
                            leaf=temp_l;
                            ip = leaf->key_lower_bound(key);
                            if(ip.i<0)
                                goto from_root;
                            p=leaf;
                            goto from_leaf;
                        }
                        else {
                            V1=V2;
                            p=leaf;
                            goto from_leaf;
                        }
                    }
                }
                V1=V2;
                goto from_leaf;
            }

            return p;
    }

    VersionNumber btree::get_version(void* node) 
    {
        return *reinterpret_cast<VersionNumber *>(reinterpret_cast<u_int64_t>(node)+24);
    }

    void btree::new_root()
    {
        if(get_version(root_).isLeaf()) {
            leaf_node* child = new leaf_node(reinterpret_cast<leaf_node*>(root_));
            child->version.unmarkRoot();
            inner_node* root = reinterpret_cast<inner_node*>(root_);
            child->parent=root;
            root->child0=child;
            root->version.unmarkLeaf();
            root->permutation.set_size(0);
            child->version.releaseBothLocks();
        }
        else {
            inner_node* child = new inner_node(reinterpret_cast<inner_node*>(root_));
            child->version.unmarkRoot();
            inner_node* root = reinterpret_cast<inner_node*>(root_);
            child->parent=root;
            void* child0 = root->child0;
            root->child0=child;
            root->permutation.set_size(0);
            
            update_parent(child0, child);
            for(int i=0; i<LEAF_WIDTH; i++) {
                update_parent(root->entry[i].link_or_value, child);
            }
            child->version.releaseBothLocks();
        }
    }

    void btree::init_root()
    {
        leaf_node *nroot;
        nroot = new leaf_node;
        nroot->version.markRoot();
        root_ = reinterpret_cast<void *>(nroot);
    }

    int btree::insert(u_int64_t key, void* value)
    {
        kv to_insert;
        void *p;
        inner_node *inner, *temp_i;
        leaf_node *leaf, *temp_l;
        VersionNumber V1, V2;
        VersionNumber *cv1, *cv2;
        key_indexed_position ip;
        bool comp;
        bool kilo=false;

        from_root:
            p=root_;
            V1 = get_version(p);
            if(V1.isLeaf())
            {
                leaf = reinterpret_cast<leaf_node *>(p);
                goto leaf_insert;
            }

        find:
            inner = reinterpret_cast<inner_node *>(p);
            p = inner->get(key);

            V2 = get_version(p);
            if( (V1!=inner->version) || (inner->version.insertLock()) ) {
                if(V1.isRoot())
                    goto from_root;
                V2=inner->version;
                if( (V2.smoVersion()!=V1.smoVersion()) || (V2.smoLock()) ) {
                    while(1) {
                        temp_i=inner->right;
                        comp=temp_i;
                        if(comp) {
                            V1=temp_i->version;
                            comp=(key>=inner->highkey);
                        }
                        if(temp_i==inner->right)
                            break;
                    }
                    if(comp) {
                        inner=temp_i;
                        if(key<inner->highkey) {
                            p=inner;
                            goto find;
                        }
                        else {
                            goto from_root;
                        }
                    } 
                    else {
                        while(1) {
                            temp_i=inner->left;
                            comp=temp_i;
                            if(comp) {
                                V1=temp_i->version;
                                comp=(key<temp_i->highkey);
                            }
                            if(temp_i==inner->left)
                                break;
                        }
                        if(comp) {
                            inner=temp_i;
                            ip = inner->key_lower_bound(key);
                            if(ip.i<0)
                                goto from_root;
                            p=inner;
                            goto find;
                        }
                        else {
                            V1=V2;
                            p=inner;
                            goto find;
                        }
                    }
                }
                V1=V2;
                goto find;
            }
            V1=V2;

            if(p==NULL)
                return 0;
            if( V1.isLeaf() )
            {
                leaf = reinterpret_cast<leaf_node *>(p);
                goto leaf_insert;
            }
            goto find;

        leaf_insert:
            if(leaf->get(key))
                return 0;

            while(leaf->version.tryInsertLock());

            if(V1.isRoot() && V1.insertVersion()!=leaf->version.insertVersion()) {
                leaf->version.releaseInsertLock();
                goto from_root;
            }
            if(leaf->right && key>=leaf->highkey) {
                V1=leaf->right->version;
                leaf=leaf->right;
                if(key<leaf->highkey) {
                    leaf->left->version.releaseInsertLock();
                    goto leaf_insert;
                }
                else {
                    leaf->left->version.releaseInsertLock();
                    goto from_root;
                }
            } 
            else {
                while(1) {
                    temp_l=leaf->left;
                    comp=temp_l;
                    if(comp) {
                        V1=temp_l->version;
                        comp=(key<temp_l->highkey);
                    }
                    if(temp_l==leaf->left)
                        break;
                }
                if(comp) {
                    ip = temp_l->key_lower_bound(key);
                    leaf->version.releaseInsertLock();
                    if(ip.i<0)
                        goto from_root;
                    leaf=temp_l;
                    goto leaf_insert;
                }
            }

            if(leaf->full())
            {
                if(leaf->version.isRoot())
                {
                    //std::cout<<"leaf root full\n";
                    leaf->version.trySMOLock();
                    new_root();
                    leaf->version.releaseBothLocks();
                    leaf = reinterpret_cast<leaf_node *>(reinterpret_cast<void *>(leaf->dummy));
                    goto leaf_insert;
                } else 
                {
                    while(leaf->version.trySMOLock());

                    if(leaf->rebalance(key,value))
                        return 1;
                    //printf("trying leaf split\n");
                    to_insert = leaf->split(key,value,cv1,cv2);
                    key = to_insert.key;
                    value = to_insert.link_or_value;
                    p = leaf;
                    inner = leaf->parent;
                    goto inner_insert;
                }
            } else 
            {
                leaf->insert(key,value);
                leaf->version.insertVersion();
                leaf->version.releaseInsertLock();
                return 1;
            }

        inner_insert:

            while(1) {
                while(inner->version.tryInsertLock());
                if(inner==*reinterpret_cast<inner_node **>(p))
                    break;
                inner->version.releaseInsertLock();
                inner=*reinterpret_cast<inner_node **>(p);
            }            
        
            if(inner->full())
            {   
                if(inner->version.isRoot())
                {
                    inner->version.trySMOLock();
                    new_root();
                    inner = reinterpret_cast<inner_node *>(inner->child0);
                    inner->parent->version.releaseBothLocks();
                    goto inner_insert;
                } else 
                {
                    while(inner->version.trySMOLock());

                    if(inner->rebalance(key,value,cv1,cv2))
                        return 1;

                    
                    to_insert = inner->split(key,value,cv1,cv2);
                    
                    key=to_insert.key;
                    value=to_insert.link_or_value;
                    p = inner;
                    inner = inner->parent;
                    goto inner_insert;
                }
            } else 
            {
                
                inner->insert(key,value);
                cv1->releaseSMOLock();
                cv2->releaseSMOLock();
                inner->version.incrementInsert();
                inner->version.releaseInsertLock();
                return 1;
            }
    }

    void* btree::operator new(size_t size)
    {
        void *ptr = RRP_malloc(size);
        memset(ptr,0,size);

        return ptr;
    }

    void btree::operator delete(void *addr)
    {
        RRP_free(addr);
    }

    void* leaf_node::operator new(size_t size)
    {
        void *ptr = RRP_malloc(size);
        memset(ptr,0,size);

#ifdef STATS
        space*=num_nodes;
        num_nodes++;
        space/=num_nodes;
#endif

        return ptr;
    }

    void leaf_node::operator delete(void *addr)
    {
        RRP_free(addr);
    }

    void* inner_node::operator new(size_t size)
    {
        void *ptr = RRP_malloc(size);
        memset(ptr,0,size);

#ifdef STATS
        space*=num_nodes;
        num_nodes++;
        space/=num_nodes;
#endif

        return ptr;
    }

    void inner_node::operator delete(void *addr)
    {
        RRP_free(addr);
    }

    u_int64_t btree::tot_nodes()
    {
        return num_nodes;
    }

    u_int64_t btree::tot_lookups()
    {
        return num_lookups;
    }

    u_int64_t btree::tot_inserts()
    {
        return num_inserts;
    }

    u_int64_t btree::tot_rebalances()
    {
        return num_rebalances;
    }

    double btree::efficiency()
    {
        return space;
    }

    void btree::print_tree()
    {
        inner_node *inner = reinterpret_cast<inner_node *>(root_);
        std::cout<<inner->size()<<std::endl;
        std::cout<<inner->version.smoLock()<<inner->version.insertLock()<<std::endl;
        inner = reinterpret_cast<inner_node *>(inner->child0);
        std::cout<<inner->size()<<std::endl;
        std::cout<<inner->version.smoLock()<<inner->version.insertLock()<<std::endl;
    }

}