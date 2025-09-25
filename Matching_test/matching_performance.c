// compilation: gcc -g -O2 -I../ompi/include/ -I../opal/include/ -I..
// -I../3rd-party/openpmix/include matching_performance.c original_matching_queue.c
// hashmap_matching_queue_with_wildcard.c hashmap_matching_queue_no_wildcard.c
// hashmap_matching_queue_overtake_wildcard.c -Wno-format ../opal/.libs/libopen-pal.so -lpthread
// -fopenmp
/*
 * PRQ/UMQ Performance Test
 * Simulates message-arrival and receive-posted operations
 * Usage: ./bench -n <num_ops> -t <tag_range> -r <rank_range>
 */
#include "../ompi/mca/pml/pml_constants.h"
#include "hashmap_matching_queue_no_wildcard.h"

#include <assert.h>
#include <getopt.h>
#include <omp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "hashmap_matching_queue_no_wildcard.h"
#include "hashmap_matching_queue_overtake_wildcard.h"
#include "hashmap_matching_queue_with_wildcard.h"
#include "original_matching_queue.h"

#include <math.h>
#include <string.h>

// switch on mpi internal locking
bool mca_pml_ob1_matching_protection = true;

typedef struct operation {
    int tag;
    int rank;
    bool is_recv;
} operation;

typedef struct experiment_result {
    // set by run_experiment
    int pq_max;
    int uq_max;
    double time; // in milliseconds
    double ops_per_sec;
    // set by caller
    char *implementation;
    char *sequence;
    bool any_tag;
    bool any_source;
} experiment_result;

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
// overload for operation
void shuffle_op(operation *array, size_t n)
{
    if (n > 1) {
        size_t i;
        for (i = 0; i < n - 1; i++) {
            size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
            operation t = array[j];
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

operation *prepare_envelopes_random(int num_ops, int num_tags, int num_ranks, bool use_wildcards)
{
    int *tags = get_value_pool(num_tags, num_tags);
    int *ranks = get_value_pool(num_ranks, num_ranks);
    if (use_wildcards) {
        tags[0] = OMPI_ANY_TAG;
        ranks[0] = OMPI_ANY_SOURCE;
    }
    int num_wildcards = 0;

    operation *values = malloc(num_ops * 2 * sizeof(struct operation));

    for (int i = 0; i < num_ops * 2; ++i) {
        int mode = rand() % 2;
        int src = ranks[rand() % num_ranks];
        int tag = tags[rand() % num_tags];
        if (tag == OMPI_ANY_TAG || src == OMPI_ANY_SOURCE) {
            mode = 1; // a wildcard operations must be a recv
            num_wildcards++;
        }

        values[i].is_recv = mode;
        values[i].rank = src;
        values[i].tag = tag;
    }
    free(tags);
    free(ranks);
    if (use_wildcards) {
        printf("Percentage of Operations with Wildcards: %.3f %%\n",
               num_wildcards / (double) (num_ops*2) * 100.0);
    }
    return values;
}

operation *get_phase(int msg_per_phase, int num_ranks, bool use_tag_wildcard,
                     bool use_rank_wildcard)
{
    operation *phase = malloc(sizeof(operation) * msg_per_phase * 2);
    int *ranks = malloc(sizeof(int) * msg_per_phase);
    int *tags = malloc(sizeof(int) * msg_per_phase);
    for (int i = 0; i < msg_per_phase; ++i) {
        ranks[i] = rand() % num_ranks;
        tags[i] = i;
    }
    shuffle(tags, msg_per_phase); // random order

    for (int i = 0; i < msg_per_phase; ++i) {
        phase[i * 2 + 0].tag = tags[i];
        phase[i * 2 + 0].rank = ranks[i];
        phase[i * 2 + 0].is_recv = true;
        phase[i * 2 + 1].tag = tags[i];
        phase[i * 2 + 1].rank = ranks[i];
        phase[i * 2 + 1].is_recv = false;
    }
    if (use_tag_wildcard && use_rank_wildcard) {
        // insert one wildcard recv
        const int i = rand() % msg_per_phase;
        phase[i * 2 + 0].tag = MPI_ANY_TAG;
        phase[i * 2 + 0].rank = MPI_ANY_SOURCE;
    } else {
        if (use_tag_wildcard) {
            // insert one wildcard recv
            const int i = rand() % msg_per_phase;
            phase[i * 2 + 0].tag = MPI_ANY_TAG;
        }
        if (use_rank_wildcard) {
            // insert one wildcard recv
            const int i = rand() % msg_per_phase;
            phase[i * 2 + 0].rank = MPI_ANY_SOURCE;
        }
    }

    free(tags);
    free(ranks);

    return phase;
}

operation *prepare_envelopes_randomized_phases(int num_phases, int msg_per_phase, int num_ranks,
                                               bool use_wildcards)
{
    operation *phase = get_phase(msg_per_phase, num_ranks, use_wildcards, use_wildcards);

    int phase_size = 2 * msg_per_phase;

    int num_ops = num_phases * msg_per_phase * 2;
    operation *values = malloc(num_phases * phase_size * sizeof(operation));

    for (int i = 0; i < num_phases; ++i) {
        // random order of operations per phase
        shuffle_op(phase, phase_size);
        memcpy(&values[i * phase_size], phase, phase_size * sizeof(operation));
    }
    free(phase);
    return values;
}

void run_experiment(const int num_phases, const int num_ops_per_phase, const operation *operations,
                    experiment_result *result, void *(*init_matching_queues)(),
                    void (*destroy_matching_queues)(void *),
                    bool (*try_match_incoming)(void *, int, int, void *),
                    bool (*try_match_receive)(void *, int, int, void *))
{
    const int phase_size = num_ops_per_phase * 2;
    void *matching_queue = init_matching_queues();

    long prq_appends = 0, prq_dequeues = 0;
    long umq_appends = 0, umq_dequeues = 0;
    int pq_size = 0, uq_size = 0;
    int pq_max = 0, uq_max = 0;

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
#pragma omp parallel reduction(+ : prq_appends, prq_dequeues, umq_appends, umq_dequeues) \
    firstprivate(pq_size, uq_size) reduction(max : pq_max, uq_max)
    {
        for (int n = 0; n < num_phases; ++n) {
#pragma omp for schedule(static)
            for (long i = 0; i < phase_size; ++i) { // *2 send and recv
                bool is_recv = operations[n * phase_size + i].is_recv;
                int src = operations[n * phase_size + i].rank;
                int tag = operations[n * phase_size + i].tag;

                void *payload = (void *) (uintptr_t) i + 1; // not null palyoad
                if (!is_recv) {
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
            } // implicit OpenMP barrier
        }
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double num_ops = num_phases * phase_size;
    double total_ms = diff_nsec(&t0, &t1) / 1e6;
    double ops_per_sec = num_ops / (total_ms / 1000.0);
    /*
    printf("Number of Operations: %.0f in %.3f ms (%.2f ops/sec)\n", num_ops, total_ms,ops_per_sec);
    printf("PRQ appends: %ld, PRQ dequeues: %ld, PRQ max size: %d\n", prq_appends, prq_dequeues,
           pq_max);
    printf("UMQ appends: %ld, UMQ dequeues: %ld, UMQ max size: %d\n", umq_appends, umq_dequeues,
           uq_max);
           */
    result->pq_max = pq_max;
    result->uq_max = uq_max;
    result->time = total_ms;
    result->ops_per_sec = ops_per_sec;

    destroy_matching_queues(matching_queue);
}

#define NUM_IMPLEMENTATIONS 4

void run_for_all_implementations(char *sequence_name, int num_phases, int num_ops_per_phase,
                                 operation *operations, bool any_tag, bool any_source,
                                 experiment_result *result)
{
    for (int i = 0; i < NUM_IMPLEMENTATIONS; ++i) {
        result[i].sequence = sequence_name;
        result[i].any_tag = any_tag;
        result[i].any_source = any_source;
    }

    int i = 0;
    result[i].implementation = "default";
    run_experiment(num_phases, num_ops_per_phase, operations, &result[i],
                   &default_init_matching_queues, &default_destroy_matching_queues,
                   &default_try_match_incoming, &default_try_match_receive);

    ++i;
    result[i].implementation = "hashmap_no_wildcard_support";
    if (any_tag || any_source) {
        // experiment not applicable
        result[i].time = 0;
        result[i].pq_max = 0;
        result[i].uq_max = 0;
        result[i].ops_per_sec = NAN;
    } else {
        run_experiment(num_phases, num_ops_per_phase, operations, &result[i],
                       &hashmap_no_wild_init_matching_queues,
                       &hashmap_no_wild_destroy_matching_queues,
                       &hashmap_no_wild_try_match_incoming, &hashmap_no_wild_try_match_receive);
    }
    ++i;
    result[i].implementation = "hashmap_overtaking_wildcard_support";
    run_experiment(num_phases, num_ops_per_phase, operations, &result[i],
                   &hashmap_overtake_wild_init_matching_queues,
                   &hashmap_overtake_wild_destroy_matching_queues,
                   &hashmap_overtake_wild_try_match_incoming,
                   &hashmap_overtake_wild_try_match_receive);
    ++i;
    result[i].implementation = "hashmap_full_wildcard_support";
    run_experiment(num_phases, num_ops_per_phase, operations, &result[i],
                   &hashmap_init_matching_queues, &hashmap_destroy_matching_queues,
                   &hashmap_try_match_incoming, &hashmap_try_match_receive);
}

// Function to write results to CSV
void write_results_to_csv(const char *filename, experiment_result *results, size_t count,
                          int num_threads)
{
    FILE *fp = fopen(filename, "w");
    if (!fp) {
        perror("Failed to open file");
        return;
    }

    // Write CSV header
    fprintf(
        fp,
        "num_threads,implementation,sequence,any_tag,any_source,pq_max,uq_max,time,ops_per_sec\n");

    // Write each row
    for (size_t i = 0; i < count; i++) {
        fprintf(fp, "%d,%s,%s,%d,%d,%d,%d,%.6f,%.6f\n", num_threads,
                results[i].implementation ? results[i].implementation : "",
                results[i].sequence ? results[i].sequence : "", results[i].any_tag ? 1 : 0,
                results[i].any_source ? 1 : 0, results[i].pq_max, results[i].uq_max,
                results[i].time, results[i].ops_per_sec);
    }

    fclose(fp);
}

#define NUM_SEQUENCES 4

int main(int argc, char **argv)
{
    int opt;
    int num_phases = 100;
    int num_tags_per_phase = 100;
    int num_ranks = 20;
    int repititions = 3;
    while ((opt = getopt(argc, argv, "n:t:p:r:")) != -1) {
        switch (opt) {
        case 'n':
            num_phases = atol(optarg);
            break;
        case 't':
            num_tags_per_phase = atoi(optarg);
            break;
        case 'p':
            num_ranks = atoi(optarg);
            break;
        case 'r':
            repititions = atoi(optarg);
            break;
        }
    }
    // init random seed
    srand((unsigned) time(NULL));

    int num_threads = 1;
#pragma omp parallel
#pragma omp single
    num_threads = omp_get_num_threads();

    printf("Run with %d Threads\n", num_threads);

    experiment_result *results = calloc(sizeof(experiment_result),  NUM_IMPLEMENTATIONS
                                        * repititions * NUM_SEQUENCES);

    for (int i = 0; i < repititions; ++i) {
        // fully random
        int sequence = 0;
        operation *operations = prepare_envelopes_random(num_phases * num_tags_per_phase,
                                                         num_tags_per_phase, num_ranks, false);
        experiment_result *res = &results[i * NUM_SEQUENCES * NUM_IMPLEMENTATIONS
                                          + sequence * NUM_IMPLEMENTATIONS];

        run_for_all_implementations("random_no_wildcard", num_phases, num_tags_per_phase,
                                    operations, false, false, res);
        free(operations);

        sequence++;
        operations = prepare_envelopes_random(num_phases * num_tags_per_phase, num_tags_per_phase,
                                              num_ranks, true);
        res = &results[i * NUM_SEQUENCES * NUM_IMPLEMENTATIONS + sequence * NUM_IMPLEMENTATIONS];
        run_for_all_implementations("random_with_wildcard", num_phases, num_tags_per_phase,
                                    operations, true, true, res);
        free(operations);

        sequence++;

        operations = prepare_envelopes_randomized_phases(num_phases, num_tags_per_phase, num_ranks,
                                                         false);
        res = &results[i * NUM_SEQUENCES * NUM_IMPLEMENTATIONS + sequence * NUM_IMPLEMENTATIONS];
        run_for_all_implementations("phase_no_wildcard", num_phases, num_tags_per_phase, operations,
                                    false, false, res);
        free(operations);

        sequence++;

        operations = prepare_envelopes_randomized_phases(num_phases, num_tags_per_phase, num_ranks,
                                                         true);
        res = &results[i * NUM_SEQUENCES * NUM_IMPLEMENTATIONS + sequence * NUM_IMPLEMENTATIONS];
        run_for_all_implementations("phase_with_wildcard", num_phases, num_tags_per_phase,
                                    operations, true, true, res);
        free(operations);
    }

    write_results_to_csv("experiment_log", results,
                         NUM_IMPLEMENTATIONS * repititions * NUM_SEQUENCES, num_threads);
    free(results);

    return 0;
}
