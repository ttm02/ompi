#ifndef MATCHING_PERFORMANCE_H
#define MATCHING_PERFORMANCE_H

#include <assert.h>
#include <stdbool.h>


// switch on mpi internal locking
extern bool mca_pml_ob1_matching_protection;

typedef struct operation {
    int tag;
    int rank;
    bool is_recv;
} operation;

typedef struct experiment_result {
    // set by run_experiment
    int pq_max;
    int uq_max;
    double pq_avg;
    double uq_avg;
    double time; // in milliseconds
    double ops_per_sec;
    // set by caller
    char *implementation;
    char *sequence;
    bool any_tag;
    bool any_source;
} experiment_result;

typedef struct implementation_info {
    char* name;
    // functions to call in experiment
    void *(*init_matching_queues)();
    void (*destroy_matching_queues)(void *);
    bool (*try_match_incoming)(void *, int, int, void *);
    bool (*try_match_receive)(void *, int, int, void *);
    struct implementation_info* next_implementation; // to build a linked list, does not need to be set when calling register_implementation
}implementation_info;

void register_implementation(const implementation_info * info);

#endif



