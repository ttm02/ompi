// compilation: gcc -O2 -g -fopenmp -I../ompi/include/ -I../opal/include/ -I..  -I../3rd-party/openpmix/include matching_performance_map.c -Wno-format ../opal/class/.libs/opal_object.o ../opal/mca/threads/base/.libs/mutex.o ../opal/mca/threads/pthreads/.libs/threads_pthreads_module.o ../opal/.libs/libopen-pal.so -lpthread -o map
/*
 * PRQ/UMQ Performance Test
 * Simulates message-arrival and receive-posted operations
 * Usage: ./bench -n <num_ops> -t <tag_range> -r <rank_range>
 */
#include <assert.h>
#include <getopt.h>
#include <omp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

// forward declare to remove problem with include ordering when including this internal header
// struct custom_match_prq;
// struct custom_match_umq;
// typedef struct custom_match_prq custom_match_prq;
// typedef struct custom_match_umq custom_match_umq;
// dont use variable, just use locking for testing
bool mca_pml_ob1_matching_protection = true;

#define NO_DEBUGGING_UNDER_PERFORMANCE_TESTING

#include "../ompi/mca/pml/ob1/custommatch/pml_ob1_custom_match_hashmap.h"

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

    int *tags = get_value_pool(num_tags, tag_pool_range);
    int *ranks = get_value_pool(num_ranks, rank_pool_range);

    hashmap *matching_map = match_map_init();

    long prq_appends = 0, prq_dequeues = 0;
    long umq_appends = 0, umq_dequeues = 0;
    int pq_size = 0, uq_size = 0;
    int pq_max = 0, uq_max = 0;

    int num_t = 1;
#pragma omp parallel
    {
#pragma omp master
        {
            num_t = omp_get_num_threads();
        }
    }

    // random seed
    srand((unsigned) time(NULL));
    unsigned int *srand_buffer = malloc(sizeof(unsigned int) * num_t);
    for (int i = 0; i < num_t; ++i) {
        srand_buffer[i] = rand(); // each T has a different seed
    }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
#pragma omp parallel for reduction(+ : prq_appends, prq_dequeues, umq_appends, umq_dequeues) \
    firstprivate(pq_size, uq_size, pq_max, uq_max)
    for (long i = 0; i < num_ops; ++i) {
        const int tag = tags[rand_r(&srand_buffer[omp_get_thread_num()]) % num_tags];
        const int src = ranks[rand_r(&srand_buffer[omp_get_thread_num()]) % num_ranks];
        void *payload = (void *) (uintptr_t) i + 1; // payload cannot be NULL
        void** to_fill =NULL;
        if (rand_r(&srand_buffer[omp_get_thread_num()]) % 2 == 1) {
            // Operation 1: receive posted
            // search posted receives (PRQ)
            void *recv_req = get_match_or_insert(matching_map, tag, src, &to_fill, false);
            if (recv_req == NULL) {
                __atomic_store_n(to_fill,payload,__ATOMIC_RELAXED);
                prq_appends++;
                pq_size++;
                if (pq_size > pq_max)
                    pq_max = pq_size;
            } else {
                umq_dequeues++;
                uq_size--;
            }
        } else {
            // Operation 2: message arrival
            void *msg_found = get_match_or_insert(matching_map, tag, src, &to_fill, true);
            if (msg_found == NULL) {
                __atomic_store_n(to_fill,payload,__ATOMIC_RELAXED);
                umq_appends++;
                uq_size++;
                if (uq_size > uq_max)
                    uq_max = uq_size;
            } else {
                prq_dequeues++;
                pq_size--;
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
    free(srand_buffer);

    match_map_destroy(matching_map);
    return 0;
}
