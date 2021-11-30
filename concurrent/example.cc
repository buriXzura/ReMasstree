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

#define NUM_THR 2

u_int64_t *keys;
void **values;
int num_tests;
masstree::btree *tree;

void *run(void* arg) {
    int num = *(int *)arg;
    int inserts=0, gets=0;
    //cout<<num<<"\n";
    int temp = num_tests/NUM_THR;
    int st = temp*num;
    int ed = st+temp;
    //return NULL;
    for(int i=st; i<ed; i++) {
        inserts+=tree->insert(keys[i],values[i]);
    }
    for(int i=st; i<ed; i++) {
        if(tree->get(keys[i])==values[i])
            gets++;
    }
    cout<<"inserts: "<<inserts<<", gets: "<<gets<<endl;
    return NULL;
}

int main(int argc, char **argv)
{
    keys = new u_int64_t[(int)1e9+3];
    values = (void **) malloc(((int)1e9+3)*sizeof(void *));

    tree = new masstree::btree;
    u_int64_t key=14;
    void *val,*ans;

    init_seed();
    num_tests=1000;
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
        keys[i]=key;
        values[i]=malloc(4);
        //cout<<"insert: "<<i<<endl;
    }

    pthread_t thr[NUM_THR];
    int tid[NUM_THR];

    for(int i=0; i<NUM_THR; i++) {
        tid[i]=i;
        pthread_create(&thr[i], NULL, run, (void*)&tid[i]);
    }
    //cout<<inserts<<"\n";
    for(int i=0; i<NUM_THR; i++)
        pthread_join(thr[i], NULL);
    //tree->print_tree();

    return 0;
}
