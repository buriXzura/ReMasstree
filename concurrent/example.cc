#include <iostream>
#include <random>
#include <time.h>

using namespace std;

#include "masstree.h"

//#define RRP_malloc RP_malloc
//#define RRP_free RP_free

//#define RRP_malloc malloc
//#define RRP_free free

#define REGION_SIZE (1024*1024*1024ULL)

static __uint128_t g_lehmer64_state;

static void init_seed(void) {
   srand(time(NULL));
   g_lehmer64_state = rand();
}

static uint64_t lehmer64() {
  g_lehmer64_state *= 0xda942042e4dd58b5;
  return g_lehmer64_state >> 64;
}


u_int64_t *keys;
void **values;



int main(int argc, char **argv)
{
    keys = new u_int64_t[(int)1e9+3];
    values = (void **) malloc(((int)1e9+3)*sizeof(void *));

    masstree::btree *tree = new masstree::btree;
    u_int64_t key=14;
    void *val,*ans;

    init_seed();
    int num_tests=1000;
    if(argc>1)
        num_tests = atoi(argv[1]);
    int success=0;
    int inserts=0;

    key=INT64_MAX;
    key=0;
    for(int i=0; i<num_tests; i++)
    {
        //key=lehmer64();
        key+=10;
        inserts+=tree->insert(key,malloc(4));
        keys[i]=key;
        //cout<<"insert: "<<i<<endl;
    }

    int a=0;
    for(int i=0; i<num_tests; i++)
    {
        key=keys[i];
        ans=tree->get(key);
        if(ans) {
            a++;
            //cout<<i<<":"<<key<<"\n";
        }
    }

    cout<<a<<"\n";
    //cout<<inserts<<"\n";

    //tree->print_tree();

    return 0;
}
