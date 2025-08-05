// compilation: gcc -g -O2 -I../ompi/include/ -I../opal/include/ -I..  -I../3rd-party/openpmix/include matching_performance.c -Wno-format ../opal/class/.libs/opal_object.o ../opal/mca/threads/base/.libs/mutex.o ../opal/mca/threads/pthreads/.libs/threads_pthreads_module.o -lpthread
/*
 * PRQ/UMQ Performance Test
 * Simulates message-arrival and receive-posted operations
 * Usage: ./bench -n <num_ops> -t <tag_range> -r <rank_range>
 */
#include <assert.h>
#include <getopt.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

// forward declare to remove problem with include ordering when including this internal header
struct custom_match_prq;
struct custom_match_umq;
typedef struct custom_match_prq custom_match_prq;
typedef struct custom_match_umq custom_match_umq;
// dont use variable, just use locking for testing
bool mca_pml_ob1_matching_protection =true;

#define NO_DEBUGGING_UNDER_PERFORMANCE_TESTING

#include "../ompi/mca/pml/ob1/custommatch/pml_ob1_custom_match_linkedlist.h"
//#include "../ompi/mca/pml/ob1/custommatch/pml_ob1_custom_match_arrays.h"

// from https://stackoverflow.com/questions/6127503/shuffle-array-in-c
/* Arrange the N elements of ARRAY in random order.
   Only effective if N is much smaller than RAND_MAX;
   if this may not be the case, use a better random
   number generator. */
void shuffle(int *array, size_t n)
{
    if (n > 1) {
        size_t i;
        for (i = 0; i < n - 1; i++) {
            size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
            int t = array[j];
            array[j] = array[i];
            array[i] = t;
        }
    }
}

static long diff_nsec(struct timespec *a, struct timespec *b)
{
    return (b->tv_sec - a->tv_sec) * 1000000000L + (b->tv_nsec - a->tv_nsec);
}

int *get_value_pool(int num_vals, int pool_range)
{
    // Pre-generate unique random tags from [0, pool_range)
    assert(num_vals <= pool_range);
    int *pool = malloc(pool_range * sizeof(int));
    for (int i = 0; i < pool_range; ++i) {
        pool[i] = i;
    }
    shuffle(pool, pool_range);
    int *values = malloc(num_vals * sizeof(int));
    for (int i = 0; i < num_vals; ++i) {
        values[i] = pool[i];
    } // select first N tags
    free(pool);
    return values;
}
int main(int argc, char **argv)
{
    int opt;
    long num_ops = 100000;
    int tag_pool_range = 1000;
    int rank_pool_range = 100;
    int num_tags = 100;
    int num_ranks = 20;
    while ((opt = getopt(argc, argv, "n:t:r:")) != -1) {
        switch (opt) {
        case 'n':
            num_ops = atol(optarg);
            break;
        case 't':
            num_tags = atoi(optarg);
            break;
        case 'r':
            num_ranks = atoi(optarg);
            break;
        }
    }
    // random seed
    srand((unsigned) time(NULL));

    int *tags = get_value_pool(num_tags, tag_pool_range);
    int *ranks = get_value_pool(num_ranks, rank_pool_range);

    custom_match_prq *pq = custom_match_prq_init();
    custom_match_umq *uq = custom_match_umq_init();

    long prq_appends = 0, prq_dequeues = 0;
    long umq_appends = 0, umq_dequeues = 0;
    int pq_size = 0, uq_size = 0;
    int pq_max = 0, uq_max = 0;

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (long i = 0; i < num_ops; ++i) {
        int tag = tags[rand() % num_tags];
        int src = ranks[rand() % num_ranks];
        void *payload = (void *) (uintptr_t) i+1; // not null palyoad
        if (rand() % 2 == 1) {
            // Operation 1: message arrival
            // search posted receives (PRQ)
            void *recv_req = custom_match_prq_find_dequeue_verify(pq, tag, src);
            prq_dequeues++;
            if (recv_req) {
                pq_size--; // removed from queue
            } else {
                // no match => enqueue to unexpected queue
                custom_match_umq_append(uq, tag, src, payload);
                umq_appends++;
                uq_size++;
                if (uq_size > uq_max)
                    uq_max = uq_size;
            }
        } else {
            // Operation 2: receive posted
            // search unexpected messages (UMQ)
            custom_match_umq_node *hold_prev;
            custom_match_umq_node *hold_elem;
            int hold_index;
            void *msg_found = custom_match_umq_find_verify_hold(uq, tag, src, &hold_prev,
                                                                &hold_elem, &hold_index);
            umq_dequeues++;
            if (msg_found) {
                // matched => do nothing else
                uq_size--;
                custom_match_umq_remove_hold(uq, hold_prev, hold_elem,
                                             hold_index); // actually remove
            } else {
                // not found => post receive into PRQ
                custom_match_prq_append(pq, payload, tag, src);
                prq_appends++;
                pq_size++;
                if (pq_size > pq_max)
                    pq_max = pq_size;
            }
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double total_ms = diff_nsec(&t0, &t1) / 1e6;
    printf("Number of Operations: %ld  in %.3f ms\n", num_ops, total_ms);
    printf("PRQ appends: %ld, PRQ dequeues: %ld, PRQ max size: %d\n", prq_appends, prq_dequeues,
           pq_max);
    printf("UMQ appends: %ld, UMQ dequeues: %ld, UMQ max size: %d\n", umq_appends, umq_dequeues,
           uq_max);

    free(tags);
    free(ranks);

    custom_match_prq_destroy(pq);
    custom_match_umq_destroy(uq);
    return 0;
}
