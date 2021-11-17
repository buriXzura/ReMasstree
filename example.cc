#include <iostream>
#include <time.h>

using namespace std;

#include "masstree.h"

static __uint128_t g_lehmer64_state;

static void init_seed(void) {
   srand(time(NULL));
   g_lehmer64_state = rand();
}

static uint64_t lehmer64() {
  g_lehmer64_state *= 0xda942042e4dd58b5;
  return g_lehmer64_state >> 64;
}

u_int64_t keys[(u_int64_t)1e8+2];

int main()
{
    masstree::btree tree;
    u_int64_t key=14;
    void *val,*ans;

    init_seed();
    int num_tests=40000000;
    int success=0;
    int inserts=0;
    int tot=0;

    //key=INT64_MAX;
    for(int i=0; i<num_tests; i++)
    {
        key=lehmer64();
        //key+=100;
        val=malloc(4);
        inserts+=tree.insert(key,val);
        keys[i]=key;
        tot++;
    }
    cout<<"number of keys: "<<inserts/1000000<<"M\n";
    cout<<"height: "<<tree.height()<<"\n";
    cout<<"node count: "<<tree.node_count()<<"\n";
    cout<<"space efficiency: "<<tree.space_used()*100<<"%\n";

    return 0;

    for(int i=0; i<num_tests; i++)
    {
        key=keys[i];
        ans=tree.get(key);
        if(ans)
            success++;
    }
    for(int i=0; i<num_tests; i++)
    {
        tree.remove(keys[i]);
    }
    cout<<"success: "<<success<<"/"<<1*num_tests<<"\n";
    cout<<"inserts: "<<inserts<<"/"<<1*num_tests<<"\n";
}