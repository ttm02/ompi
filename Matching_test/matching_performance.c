// compilation: gcc -g -O2 -I../ompi/include/ -I../opal/include/ -I..
// -I../3rd-party/openpmix/include matching_performance.c -Wno-format ../opal/.libs/libopen-pal.so
// -lpthread -fopenmp
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

#ifndef USE_HASHMAP
#    include "original_matching_queue.h"
#else
#    include "hashmap_matching_queue.h"
#endif

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
        srand_buffer[i] = rand();
    }

    int *tags = get_value_pool(num_tags, tag_pool_range);
    int *ranks = get_value_pool(num_ranks, rank_pool_range);

    matching_data *matching_queue = init_matching_queues();

    long prq_appends = 0, prq_dequeues = 0;
    long umq_appends = 0, umq_dequeues = 0;
    int pq_size = 0, uq_size = 0;
    int pq_max = 0, uq_max = 0;

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
#pragma omp parallel for schedule(static)                               \
    reduction(+ : prq_appends, prq_dequeues, umq_appends, umq_dequeues) \
    firstprivate(pq_size, uq_size) reduction(max : pq_max, uq_max)
    for (long i = 0; i < num_ops; ++i) {
        int tag = tags[rand_r(&srand_buffer[omp_get_thread_num()]) % num_tags];
        int src = ranks[rand_r(&srand_buffer[omp_get_thread_num()]) % num_ranks];
        void *payload = (void *) (uintptr_t) i + 1; // not null palyoad
        if (rand_r(&srand_buffer[omp_get_thread_num()]) % 2 == 1) {
            // Operation 1: message arrival
            // search posted receives (PRQ)
            if (try_match_incoming(matching_queue, tag, src, payload)) {
                --pq_size;
                prq_dequeues++;
            } else {
                umq_appends++;
                ++uq_size;
                if (uq_size > uq_max) {
                    uq_max = uq_size;
                }
            }
        } else {
            // Operation 2: receive posted
            // search unexpected messages (UMQ)

            if (try_match_receive(matching_queue, tag, src, payload)) {
                // matched => do nothing else
                umq_dequeues++;
                uq_size--;
            } else {
                // not found => post receive into PRQ
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

    destroy_matching_queues(matching_queue);
    return 0;
}
