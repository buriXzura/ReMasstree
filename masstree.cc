#include "masstree.h"

namespace masstree
{
    static constexpr uint64_t CACHE_LINE_SIZE = 64;
    u_int64_t num_nodes=0;
    double space=0;

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

        space=(space*num_nodes+1.0/capacity())/num_nodes;
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

        inner_node **value_par;
        value_par = reinterpret_cast<inner_node **>(value);
        *value_par=this;

        space=(space*num_nodes+1.0/capacity())/num_nodes;
    }

    int leaf_node::remove(u_int64_t key)
    {
        permuter temp = permutation.value();
        key_indexed_position ip = key_lower_bound(key);
        if(ip.i<0)
            return 0;
        if(compare_key(entry[ip.p].key,key)==0)
        {
            temp.remove(ip.i);
            permutation = temp.value();
            clflush((char *)&permutation, sizeof(permuter), false, true);
            free(entry[ip.p].link_or_value);
            return 1;
        } 
        return 0;
    }

    int inner_node::remove(u_int64_t key)
    {
        permuter temp = permutation.value();
        key_indexed_position ip = key_lower_bound_by(key);
        if(ip.i==temp.size())
        {
            if(compare_key(highest,key)!=0)
                return 0;
        } else 
        {
            if(compare_key(entry[ip.p].key,key)!=0)
                return 0;
        }
        if(ip.i==0)
        {
            void *snap = child0;
            if(temp.size()==0)
            {
                child0=NULL;
                clflush((char *)&child0, sizeof(void *), false, true);
                free(snap);
                return 1;
            }
            child0 = entry[ip.p].link_or_value;
            clflush((char *)&child0, sizeof(void *), false, true);
            temp.remove(0);
            permutation = temp.value();
            clflush((char *)&permutation, sizeof(permuter), false, true);
            free(snap);
        } else 
        {
            void *snap = entry[temp[ip.i-1]].link_or_value;
            temp.remove(ip.i-1);
            permutation = temp.value();
            clflush((char *)&permutation, sizeof(permuter), false, true);
            free(snap);
        }
        return 1;
    }

    void leaf_node::del()
    {
        if(right)
        {
            right->left=left;
            clflush((char *)&right->left, sizeof(leaf_node *), false, true);
        }
        if(left)
        {
            left->right=right;
            clflush((char *)&left->right, sizeof(leaf_node *), false, true);
        }
    }

    void inner_node::del()
    {
        if(right)
        {
            right->left=left;
            clflush((char *)&right->left, sizeof(inner_node *), false, true);
        }
        if(left)
        {
            left->right=right;
            clflush((char *)&left->right, sizeof(inner_node *), false, true);
        }
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
        //printf("%d,%lu\n",ip.i,entry[ip.p].key);
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
            if(compare_key(key,highest)==0)
                return entry[permutation[ip.i-1]].link_or_value;
            return NULL;
        }
        if(compare_key(key,entry[ip.p].key)!=0)
            return NULL;
        if(ip.i==0)
            return child0;
        return entry[permutation[ip.i-1]].link_or_value;
    }

    kv leaf_node::split(u_int64_t key, void *value)
    {
        int mid=size()+1;
        mid/=2;

        permuter temp=permutation.value();

        leaf_node *nr;
        nr = new leaf_node(parent,right,this,0);

        permuter nper = temp.value();
        nper.rotate(0,mid);
        nper.set_size(size()-mid);
        nr->permutation=nper.value();
        nr->highest=highest;

        for(int i=mid; i<size(); i++)
            nr->entry[temp[i]]=entry[temp[i]];
        
        clflush((char *)nr, sizeof(leaf_node), false, true);
        
        highest=entry[temp[mid]].key;
        clflush((char *)&highest, sizeof(u_int64_t), false, true);
        right=nr;
        clflush((char *)&right, sizeof(leaf_node *), false, true);
        if(nr->right)
        {
            nr->right->left=nr;
            clflush((char *)&nr->right->left, sizeof(leaf_node *), false, true);
        }
        permutation.set_size(mid);
        clflush((char *)&permutation, sizeof(permuter), false, true);

        if(compare_key(key,highest)<0)
            insert(key,value);
        else 
            nr->insert(key,value);

        return kv(highest,reinterpret_cast<void *>(nr));
    }

    kv inner_node::split(u_int64_t key, void* value)
    {
        int mid=size()+1;
        mid/=2;

        permuter temp=permutation.value();

        inner_node *nr;
        nr = new inner_node(parent,right,this,level_);

        permuter nper = temp.value();
        nper.rotate(0,mid);
        nper.set_size(temp.size()-mid);
        nr->permutation=nper.value();
        nr->highest=highest;

        nr->child0=entry[temp[mid-1]].link_or_value;
        for(int i=mid; i<temp.size(); i++)
            nr->entry[temp[i]]=entry[temp[i]];
        
        clflush((char *)nr, sizeof(inner_node), false, true);

        highest=entry[temp[mid-1]].key;
        clflush((char *)&highest, sizeof(u_int64_t), false, true);
        right=nr;
        clflush((char *)&right, sizeof(inner_node *), false, true);
        if(nr->right)
        {
            nr->right->left=nr;
            clflush((char *)&nr->right->left, sizeof(inner_node *), false, true);
        }
        permutation.set_size(mid-1);
        clflush((char *)&permutation, sizeof(permuter), false, true);

        for(int i=mid-1; i<temp.size(); i++)
        {
            inner_node **value_par;
            value_par = reinterpret_cast<inner_node **>(entry[temp[i]].link_or_value);
            *value_par = nr;
            clflush((char *)value_par, sizeof(inner_node *), false, true);
        }

        int cmp=compare_key(key,highest);
        if(cmp<0)
            insert(key,value);
        else if(cmp>0)
            nr->insert(key,value);
        
        return kv(highest,reinterpret_cast<void *>(nr));
    }

    int leaf_node::rebalance(u_int64_t key, void* value)
    {
        #ifndef REBALANCE
        return 0;
        #endif

        key_indexed_position ip = key_lower_bound_by(key);
        int mx_sze=LEAF_WIDTH;
        int to_mov;
        
        left_sibing:
            //goto right_sibling;
            if(left==NULL)
                goto right_sibling;
            if(left->parent!=parent)
                goto right_sibling;
            if(left->size()==mx_sze)
                goto right_sibling;
            
            to_mov = mx_sze-left->size();
            to_mov /= (1+(ip.i<mx_sze));
            to_mov = to_mov > 1 ? to_mov : 1;

            if(left->size()+to_mov<=mx_sze && to_mov<=ip.i)
            {
                int base=left->size();
                permuter temp = left->permutation.value();
                for(int i=0; i<to_mov; i++)
                {
                    left->entry[temp[i+base]]=entry[permutation[i]];
                    clflush((char *)&left->entry[temp[i+base]], sizeof(kv), false, true);
                }
                temp.set_size(base+to_mov);
                left->permutation=temp.value();
                clflush((char *)&left->permutation, sizeof(permuter), false, true);

                key_indexed_position p_upd = parent->key_lower_bound_by(left->highest);

                if(to_mov==ip.i)
                {
                    parent->entry[p_upd.p].key=key;
                    clflush((char *)&parent->entry[p_upd.p].key, sizeof(u_int64_t), false, true);

                    temp = permutation.value();
                    temp.rotate(0,to_mov);
                    temp.set_size(temp.size()-to_mov);
                    permutation=temp.value();
                    clflush((char *)&permutation, sizeof(permuter), false, true);

                    int pos=temp.insert_from_back(0);
                    entry[pos].key=key;
                    entry[pos].link_or_value=value;
                    clflush((char *)&entry[pos], sizeof(kv), false, true);
                    permutation=temp.value();
                    clflush((char *)&permutation, sizeof(permuter), false, true);

                    left->highest=key;
                    clflush((char *)&left->highest, sizeof(u_int64_t), false, true);

                    space=(space*num_nodes+1.0/leaf_node::capacity())/num_nodes;
                } else {
                    parent->entry[p_upd.p].key=entry[permutation[to_mov]].key;
                    clflush((char *)&parent->entry[p_upd.p].key, sizeof(u_int64_t), false, true);

                    temp = permutation.value();
                    temp.rotate(0,to_mov);
                    temp.set_size(temp.size()-to_mov);
                    permutation=temp.value();
                    clflush((char *)&permutation, sizeof(permuter), false, true);

                    left->highest=entry[temp[0]].key;
                    clflush((char *)&left->highest, sizeof(u_int64_t), false, true);
                    insert(key,value);
                }
                return 1;
            }

        right_sibling:
            if(right==NULL)
                goto end;
            if(right->parent!=parent)
                goto end;
            if(right->size()==mx_sze)
                goto end;
            
            to_mov = mx_sze-right->size();
            to_mov /= (1+(ip.i>0));
            to_mov = to_mov > 1 ? to_mov : 1;

            if(right->size()+to_mov<=mx_sze && to_mov<=mx_sze-ip.i)
            {
                int base=right->size();
                permuter temp = right->permutation.value();
                temp.rotate(0,mx_sze-to_mov);
                temp.set_size(base+to_mov);
                for(int i=0; i<to_mov; i++)
                {
                    right->entry[temp[i]]=entry[permutation[mx_sze-to_mov+i]];
                    clflush((char *)&right->entry[temp[i]], sizeof(kv), false, true);
                }
                right->permutation=temp.value();
                clflush((char *)&right->permutation, sizeof(permuter), false, true);

                key_indexed_position p_upd = parent->key_lower_bound_by(highest);
                parent->entry[p_upd.p].key=right->entry[temp[0]].key;
                clflush((char *)&parent->entry[p_upd.p].key, sizeof(u_int64_t), false, true);

                permutation.set_size(mx_sze-to_mov);

                highest=right->entry[temp[0]].key;
                clflush((char *)&highest, sizeof(u_int64_t), false, true);
                insert(key,value);
                return 1;
            }

        end:
            return 0;
    }

    int inner_node::rebalance(u_int64_t key, void* value)
    {
        #ifndef REBALANCE
        return 0;
        #endif
        
        key_indexed_position ip = key_lower_bound_by(key);
        int mx_sze=LEAF_WIDTH+1;
        int to_mov;
        int added=0;

        left_sibling:
            if(left==NULL)
                goto right_sibling;
            if(left->parent!=parent)
                goto right_sibling;
            if(left->size()==mx_sze)
                goto right_sibling;

            to_mov=mx_sze-left->size();
            to_mov/=(1+(ip.i<mx_sze-1));
            to_mov = to_mov > 1 ? to_mov : 1;

            if(left->size()+to_mov<=mx_sze && to_mov<=ip.i+1)
            {
                permuter temp = left->permutation.value();
                int base=temp.size();
                key_indexed_position p_upd = parent->key_lower_bound_by(left->highest);
                
                temp.set_size(base+to_mov);
                for(int i=1; i<to_mov; i++)
                {
                    left->entry[temp[base+i]]=entry[permutation[i-1]];
                    clflush((char *)&left->entry[temp[base+i]], sizeof(kv), false, true);
                }
                left->entry[temp[base]].key=parent->entry[p_upd.p].key;
                left->entry[temp[base]].link_or_value=child0;
                clflush((char *)&left->entry[temp[base]], sizeof(kv), false, true);
                left->permutation=temp.value();
                clflush((char *)&left->permutation, sizeof(permuter), false, true);

                if(ip.i+1==to_mov)
                {
                    parent->entry[p_upd.p].key=key;
                    clflush((char *)&parent->entry[p_upd.p].key, sizeof(u_int64_t), false, true);

                    child0=value;
                    clflush((char *)&child0, sizeof(void *), false, true);
                    {
                        inner_node **value_par;
                        value_par = reinterpret_cast<inner_node **>(child0);
                        *value_par = this;
                        clflush((char *)value_par, sizeof(inner_node *), false, true);
                    }
                    temp = permutation.value();
                    temp.set_size(temp.size()-to_mov+1);
                    temp.rotate(0,to_mov-1);
                    permutation = temp.value();
                    clflush((char *)&permutation, sizeof(permuter), false, true);

                    temp=left->permutation.value();
                    for(int i=base; i<base+to_mov; i++)
                    {
                        inner_node **value_par;
                        value_par = reinterpret_cast<inner_node **>(left->entry[temp[i]].link_or_value);
                        *value_par = left;
                        clflush((char *)value_par, sizeof(inner_node *), false, true);
                    }

                    left->highest=parent->entry[p_upd.p].key;
                    clflush((char *)&left->highest, sizeof(u_int64_t), false, true);

                    space=(space*num_nodes+1.0/inner_node::capacity())/num_nodes;
                } else 
                {
                    temp = permutation.value();
                    parent->entry[p_upd.p].key=entry[temp[to_mov-1]].key;
                    clflush((char *)&parent->entry[p_upd.p].key, sizeof(u_int64_t), false, true);

                    child0=entry[temp[to_mov-1]].link_or_value;
                    clflush((char *)&child0, sizeof(void *), false, true);
                    temp.set_size(temp.size()-to_mov);
                    temp.rotate(0,to_mov);
                    permutation = temp.value();
                    clflush((char *)&permutation, sizeof(permuter), false, true);

                    temp = left->permutation.value();
                    for(int i=base; i<base+to_mov; i++)
                    {
                        inner_node **value_par;
                        value_par = reinterpret_cast<inner_node **>(left->entry[temp[i]].link_or_value);
                        *value_par = left;
                        clflush((char *)value_par, sizeof(inner_node *), false, true);
                    }

                    left->highest=parent->entry[p_upd.p].key;
                    clflush((char *)&left->highest, sizeof(u_int64_t), false, true);
                    insert(key,value);
                }
                return 1;
            }
                                                                           
        right_sibling:
            if(right==NULL)
                goto end;
            if(right->parent!=parent)
                goto end;
            if(right->size()==mx_sze)
                goto end;
            
            to_mov=mx_sze-right->size();
            to_mov/=2;
            to_mov = to_mov > 1 ? to_mov : 1;

            if(right->size()+to_mov<=mx_sze && to_mov<mx_sze-ip.i)
            {
                int base=right->size();
                permuter temp = right->permutation.value();
                temp.rotate(0,LEAF_WIDTH-to_mov);
                temp.set_size(temp.size()+to_mov);
                for(int i=0; i<to_mov-1; i++)
                {
                    right->entry[temp[i]]=entry[permutation[mx_sze-to_mov+i]];
                    clflush((char *)&right->entry[temp[i]], sizeof(kv), false, true);
                }
                right->entry[temp[to_mov-1]].key=highest;
                right->entry[temp[to_mov-1]].link_or_value=right->child0;
                clflush((char *)&right->entry[temp[to_mov-1]], sizeof(kv), false, true);
                right->permutation=temp.value();
                clflush((char *)&right->permutation, sizeof(permuter), false, true);
                right->child0=entry[permutation[LEAF_WIDTH-to_mov]].link_or_value;
                clflush((char *)&right->child0, sizeof(void *), false, true);

                key_indexed_position p_upd = parent->key_lower_bound_by(highest);
                parent->entry[p_upd.p].key = entry[permutation[LEAF_WIDTH-to_mov]].key;
                clflush((char *)&parent->entry[p_upd.p].key, sizeof(u_int64_t), false, true);

                permutation.set_size(LEAF_WIDTH-to_mov);
                clflush((char *)&permutation, sizeof(permuter), false, true);

                for(int i=LEAF_WIDTH-to_mov; i<LEAF_WIDTH; i++)
                {
                    inner_node **value_par;
                    value_par = reinterpret_cast<inner_node **>(entry[permutation[i]].link_or_value);
                    *value_par = right;
                    clflush((char *)value_par, sizeof(inner_node *), false, true);
                }

                highest=parent->entry[p_upd.p].key;
                clflush((char *)&highest, sizeof(u_int64_t), false, true);
                insert(key,value);
                return 1;
            }

        end:
            return 0;
    }

    void* btree::get(u_int64_t key)
    {
        void *p = root_;
        inner_node *inner;
        leaf_node *leaf;

        if(level(p)==0)
            goto from_leaf;

        from_inner:
            inner = reinterpret_cast<inner_node *>(p);
            p = inner->get(key);
            if(p==NULL)
                return NULL;
            if(inner->level_==1)
                goto from_leaf;
            goto from_inner;

        from_leaf:
            leaf = reinterpret_cast<leaf_node *>(p);
            p = leaf->get(key);
            return p;
    }

    int btree::level(void* node)
    {
        int level;
        level = *reinterpret_cast<u_int32_t *>(reinterpret_cast<u_int64_t>(node)+60);
        return level;
    }

    void btree::new_root()
    {
        void *root = root_;
        int lvl = level(root);
        inner_node *nroot;
        nroot = new inner_node;
        nroot->level_=lvl+1;
        nroot->child0=root;
        clflush((char *)nroot, sizeof(inner_node),false,true);

        inner_node **value_par;
        value_par = reinterpret_cast<inner_node **>(root);
        *value_par = nroot;
        clflush((char *)value_par,sizeof(inner_node *),false,true);

        root_ = reinterpret_cast<void *>(nroot);
        clflush((char *)&root_,sizeof(void *),false,true);

        space = (space*num_nodes+1.0/inner_node::capacity())/num_nodes;
    }

    void btree::init_root()
    {
        leaf_node *nroot;
        nroot = new leaf_node;
        clflush((char *)nroot, sizeof(leaf_node), false, true);
        root_ = reinterpret_cast<void *>(nroot);
        clflush((char *)&root_, sizeof(void *), false, true);
    }

    int btree::insert(u_int64_t key, void* value)
    {
        kv to_insert;
        void *p=root_;
        inner_node *inner;
        leaf_node *leaf;

        if(level(p)==0)
        {
            leaf = reinterpret_cast<leaf_node *>(p);
            //printf("root leaf\n");
            goto leaf_insert;
        }

        find:
            inner = reinterpret_cast<inner_node *>(p);
            //printf("searching inner\n");
            p = inner->get(key);
            //printf("search is over\n");
            if(p==NULL)
                return 0;
            if(inner->level_==1)
            {
                leaf = reinterpret_cast<leaf_node *>(p);
                goto leaf_insert;
            }
            goto find;

        leaf_insert:
            if(leaf->get(key))
                return 0;
            if(leaf->full())
            {
                if(leaf->parent==NULL)
                {
                    new_root();
                    goto leaf_insert;
                } else 
                {
                    //printf("trying leaf rebalancing\n");
                    if(leaf->rebalance(key,value))
                        return 1;
                    //printf("trying leaf split\n");
                    to_insert = leaf->split(key,value);
                    key = to_insert.key;
                    value = to_insert.link_or_value;
                    
                    inner = leaf->parent;
                    goto inner_insert;
                }
            } else 
            {
                //printf("leaf inserting\n");
                leaf->insert(key,value);
                //printf("leaf inserted\n");
                return 1;
            }

        inner_insert:
            if(inner->full())
            {
                if(inner->parent==NULL)
                {
                    new_root();
                    goto inner_insert;
                } else 
                {
                    //printf("trying inner rebalance\n");
                    if(inner->rebalance(key,value))
                        return 1;

                    //printf("trying inner split\n");
                    to_insert = inner->split(key,value);
                    key=to_insert.key;
                    value=to_insert.link_or_value;

                    inner = inner->parent;
                    goto inner_insert;
                }
            } else 
            {
                //printf("inner inserting\n");
                inner->insert(key,value);
                //printf("inner inserted\n");
                return 1;
            }
    }

    void btree::remove(u_int64_t key)
    {
        void *p=root_;
        inner_node* inner;
        leaf_node* leaf;

        if(level(p)==0)
        {
            leaf = reinterpret_cast<leaf_node *>(p);
            goto leaf_delete;
        }

        find:
            inner = reinterpret_cast<inner_node *>(p);
            p = inner->get(key);
            if(p==NULL)
                return;
            if(inner->level_==1)
            {
                leaf = reinterpret_cast<leaf_node *>(p);
                goto leaf_delete;
            }
            goto find;

        leaf_delete:
            if(!leaf->remove(key))
                return;
            if(!leaf->empty())
                return;
            if(leaf->parent==NULL)
                return;
            
            leaf->del();
            key = leaf->highest;
            inner = leaf->parent;

        inner_delete:
            inner->remove(key);
            if(!inner->empty())
                return;
            
            if(inner->parent==NULL)
            {
                void *snap = root_;
                init_root();
                free(snap);
                return;
            }
            
            inner->del();
            key = inner->highest;
            inner = inner->parent;
            goto inner_delete;
    }

    void* btree::operator new(size_t size)
    {
        void *ptr = malloc(size);
        memset(ptr,0,size);
        return ptr;
    }

    void btree::operator delete(void *addr)
    {
        free(addr);
    }

    void* leaf_node::operator new(size_t size)
    {
        void *ptr = malloc(size);
        memset(ptr,0,size);
        
        space=space*num_nodes;
        num_nodes++;
        space/=num_nodes;

        return ptr;
    }

    void leaf_node::operator delete(void *addr)
    {
        free(addr);
    }

    void* inner_node::operator new(size_t size)
    {
        void *ptr = malloc(size);
        memset(ptr,0,size);
        
        space=space*num_nodes;
        num_nodes++;
        space/=num_nodes;

        return ptr;
    }

    void inner_node::operator delete(void *addr)
    {
        free(addr);
    }

    u_int64_t btree::node_count()
    {
        return num_nodes;
    }

    double btree::space_used()
    {
        return space;
    }

}