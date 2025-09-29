// compilation: gcc -g -O3 -march=native -mtune=native -fopenmp -I../ompi/include/
// -I../opal/include/ -I.. -I../3rd-party/openpmix/include matching_performance.c
// original_matching_queue.c hashmap_matching_queue_*.c -Wno-format ../opal/.libs/libopen-pal.so
// -lpthread

/*
 * PRQ/UMQ Performance Test
 * Simulates message-arrival and receive-posted operations
 * Usage: ./bench -n <num_ops> -t <tag_range> -r <rank_range>
 */
#include "matching_performance.h"

#include "../ompi/mca/pml/pml_constants.h"

#include <assert.h>
#include <dirent.h>
#include <getopt.h>
#include <omp.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <math.h>
#include <string.h>

// switch on mpi internal locking
bool mca_pml_ob1_matching_protection = true;

implementation_info *implementation_list_head = NULL;
int implementation_list_size = 0;

sequence_info *sequence_list_head = NULL;
int sequence_list_size = 0;

experiment_result* experiment_result_list_head=NULL;

// read in an event file, create one sequence for each communicator used
static inline void read_events_from_file(char* dirname, char *filename)
    {
    FILE *f = fopen(filename, "rb");
    if (!f) {
        perror("fopen");
        return;
    }

    // Read num_communicators
    int32_t num_comms;
    if (fread(&num_comms, sizeof(int32_t), 1, f) != 1) {
        perror("fread num_communicators");
        fclose(f);
        return;
    }

    int32_t* event_count = calloc(num_comms, sizeof(int32_t));

    // Read event_count array
    if (fread(event_count, sizeof(int32_t), num_comms, f) != (size_t) num_comms) {
        perror("fread event_count");
        fclose(f);
        return;
    }

    // Read events for each communicator
    for (int i = 0; i < num_comms; i++) {
        int count = event_count[i];
        if (count > 0) {
            operation* ops = malloc(sizeof(struct operation) * count);
            if (!ops) {
                perror("malloc events");
                fclose(f);
                return;
            }
            if (fread(ops, sizeof(struct operation), count, f)
                != (size_t) count) {
                perror("fread events");
                fclose(f);
                return;
            }
            //register
            sequence_info sequence;
            sequence.name= dirname;
            sequence.num_phases=1;
            sequence.phase_size=count;
            sequence.ops = ops;
            //TODO actually check for wildcards
            sequence.has_any_source=false;
            sequence.has_any_tag=false;
            register_sequence(&sequence);
        }
    }
    free(event_count);


    fclose(f);

}

void read_events_from_dir(char *dirname)
{
    DIR *dir = opendir(dirname);
    if (!dir) {
        perror("opendir");
        return;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        // skip "." and ".."
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;

        // only process files starting with "events_" and ending with ".bin"
        if (strncmp(entry->d_name, "events_", 7) == 0) {
            size_t len = strlen(entry->d_name);
            if (len > 4 && strcmp(entry->d_name + len - 4, ".bin") == 0) {
                // construct full path
                char path[PATH_MAX];
                snprintf(path, sizeof(path), "%s/%s", dirname, entry->d_name);

                printf("Reading events from: %s\n", path);
                read_events_from_file(dirname,path);
            }
        }
    }

    closedir(dir);
}

void register_implementation(const implementation_info *info)
{
    implementation_info *new_info = (implementation_info *) malloc(sizeof(implementation_info));
    memcpy(new_info, info, sizeof(implementation_info));

    new_info->next_implementation = implementation_list_head;
    implementation_list_head = new_info;
    implementation_list_size++;
}

void register_sequence(const sequence_info *info)
{
    sequence_info *new_info = (sequence_info *) malloc(sizeof(sequence_info));
    memcpy(new_info, info, sizeof(sequence_info));

    new_info->next = sequence_list_head;
    sequence_list_head = new_info;
    sequence_list_size++;
}

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

void split_phase(operation *phase, int op_per_phase, bool revc_first)
{
    operation *temp = malloc(sizeof(operation) * 2 * op_per_phase);
    int begin = 0;
    int end = op_per_phase * 2 - 1;

    for (int i = 0; i < op_per_phase; ++i) {
        if (revc_first) {
            temp[begin++] = phase[i * 2 + 0];
            temp[end--] = phase[i * 2 + 1];
        } else {
            temp[end--] = phase[i * 2 + 0];
            temp[begin++] = phase[i * 2 + 1];
        }
    }
    assert(begin == end + 1);

    memcpy(phase, temp, op_per_phase * 2 * sizeof(operation));
    free(temp);
}

// random order of operations per phase, all phases interleaved: basically totally random, but we
// know all mgs muts match at some time
operation *prepare_envelopes_random(int num_phases, int msg_per_phase, int num_ranks,
                                    bool use_wildcards)
{
    operation *phase = get_phase(msg_per_phase, num_ranks, use_wildcards, use_wildcards);
    int phase_size = 2 * msg_per_phase;

    operation *values = malloc(num_phases * phase_size * sizeof(operation));
    for (int i = 0; i < num_phases; ++i) {
        memcpy(&values[i * phase_size], phase, phase_size * sizeof(operation));
    }
    free(phase);

    int num_ops = num_phases * msg_per_phase * 2;
    shuffle_op(values, num_ops);
    return values;
}

// random order of operations per phase
operation *prepare_envelopes_randomized_phases(int num_phases, int msg_per_phase, int num_ranks,
                                               bool use_wildcards)
{
    operation *phase = get_phase(msg_per_phase, num_ranks, use_wildcards, use_wildcards);

    int phase_size = 2 * msg_per_phase;

    int num_ops = num_phases * msg_per_phase * 2;
    operation *values = malloc(num_phases * phase_size * sizeof(operation));

    for (int i = 0; i < num_phases; ++i) {

        shuffle_op(phase, phase_size);
        memcpy(&values[i * phase_size], phase, phase_size * sizeof(operation));
    }
    free(phase);
    return values;
}

// all recv are posted before msg arrival
operation *prepare_envelopes_rsend_phases(int num_phases, int msg_per_phase, int num_ranks,
                                          bool use_wildcards)
{
    operation *phase = get_phase(msg_per_phase, num_ranks, use_wildcards, use_wildcards);
    split_phase(phase, msg_per_phase, true);
    int phase_size = 2 * msg_per_phase;
    operation *values = malloc(num_phases * phase_size * sizeof(operation));

    for (int i = 0; i < num_phases; ++i) {
        // randomize each send and recv part?
        memcpy(&values[i * phase_size], phase, phase_size * sizeof(operation));
    }
    free(phase);
    return values;
}

// all msg arrive before recv is posted
operation *prepare_envelopes_unexpected_phases(int num_phases, int msg_per_phase, int num_ranks,
                                               bool use_wildcards)
{
    operation *phase = get_phase(msg_per_phase, num_ranks, use_wildcards, use_wildcards);
    split_phase(phase, msg_per_phase, false);
    int phase_size = 2 * msg_per_phase;

    operation *values = malloc(num_phases * phase_size * sizeof(operation));

    for (int i = 0; i < num_phases; ++i) {
        // randomize each send and recv part?
        memcpy(&values[i * phase_size], phase, phase_size * sizeof(operation));
    }
    free(phase);
    return values;
}

// matching operations right after one another, the recv is posted fir
operation *prepare_envelopes_perfect_phases(int num_phases, int msg_per_phase, int num_ranks,
                                            bool use_wildcards)
{
    operation *phase = get_phase(msg_per_phase, num_ranks, use_wildcards, use_wildcards);
    int phase_size = 2 * msg_per_phase;

    operation *values = malloc(num_phases * phase_size * sizeof(operation));

    for (int i = 0; i < num_phases; ++i) {
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
    const int phase_size = num_ops_per_phase;
    void *matching_queue = init_matching_queues();

    long prq_appends = 0, prq_dequeues = 0;
    long umq_appends = 0, umq_dequeues = 0;
    int pq_size = 0, uq_size = 0;
    unsigned long average_prq_size = 0, average_umq_size = 0;
    bool average_overflow=false;
    int pq_max = 0, uq_max = 0;

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
#pragma omp parallel reduction(+ : prq_appends, prq_dequeues, umq_appends, umq_dequeues, \
                                   average_prq_size, average_umq_size)                   \
    firstprivate(pq_size, uq_size) reduction(max : pq_max, uq_max)
    {
        for (int n = 0; n < num_phases; ++n) {
#pragma omp for schedule(static, 1)
            for (long i = 0; i < phase_size; ++i) {
                bool is_recv = operations[n * phase_size + i].is_recv;
                int src = operations[n * phase_size + i].rank;
                int tag = operations[n * phase_size + i].tag;

                void *payload = (void *) (uintptr_t) i + 1; // not null palyoad
                if (!is_recv) {
                    // "problem" in the benchmark, for short queue sizes, this branch is
                    // unpredictable e.g. the corresponding recv is issued fast this means that
                    // the runtime for smaller queue sizes is larger due to branch
                    // missprediction as the benchmakr is also about the number of brnahces in
                    // the matching impl, this does show in resuilt data as the exact same
                    // sequence is used, it does not inhibit comparability the effect goes away
                    // on longer sequences

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
                average_prq_size += pq_size;
                average_umq_size += uq_size;
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
    result->pq_avg = average_prq_size / num_ops;
    result->uq_avg = average_umq_size / num_ops;
    result->pq_max = pq_max;
    result->uq_max = uq_max;
    result->time = total_ms;
    result->ops_per_sec = ops_per_sec;

    destroy_matching_queues(matching_queue);
}

void run_for_all_implementations(char *sequence_name, int num_phases, int num_ops_per_phase,
                                 operation *operations, bool any_tag, bool any_source)
{

    implementation_info *impl = implementation_list_head;
    for (int i = 0; i < implementation_list_size; ++i) {
        experiment_result* result = calloc(1, sizeof(experiment_result));
        assert(impl != NULL);
        result->sequence = sequence_name;
        result->any_tag = any_tag;
        result->any_source = any_source;
        result->implementation = impl->name;

        if ((any_tag || any_source) && strstr(impl->name, "no_wild") != NULL) {
            // "no_wild" is contained in implementation->name
            // experiment not applicable in implementation does not support wildcards
            result->time = NAN;
            result->ops_per_sec = NAN;
            result->pq_avg = NAN;
            result->uq_avg = NAN;
            result->pq_max = 0;
            result->uq_max = 0;

        } else {
            run_experiment(num_phases, num_ops_per_phase, operations, result,
                           impl->init_matching_queues, impl->destroy_matching_queues,
                           impl->try_match_incoming, impl->try_match_receive);
        }
        result->next=experiment_result_list_head;
        experiment_result_list_head = result;
        impl = impl->next_implementation;
    }
}

// Function to write results to CSV
void write_results_to_csv(const char *filename,
                          int num_threads)
{
    printf("Write results to %s\n", filename);
    FILE *fp = fopen(filename, "w");
    if (!fp) {
        perror("Failed to open file");
        return;
    }

    // Write CSV header
    fprintf(fp, "num_threads,implementation,sequence,any_tag,any_source,pq_max,uq_max,pq_avg,uq_"
                "avg,time,ops_per_sec\n");

    // Write each row
    experiment_result* result = experiment_result_list_head;
    while (result!=NULL) {
        fprintf(fp, "%d,%s,%s,%d,%d,%d,%d,%.2f,%.2f,%.6f,%.6f\n", num_threads,
                result->implementation ? result->implementation : "",
                result->sequence ? result->sequence : "", result->any_tag ? 1 : 0,
                result->any_source ? 1 : 0, result->pq_max, result->uq_max,
                result->pq_avg, result->uq_avg, result->time, result->ops_per_sec);
        experiment_result* old_result=result;
        result = old_result->next;
        free(old_result);
    }

    fclose(fp);
}

int main(int argc, char **argv)
{

    int opt;
    int num_phases = 100;
    int num_tags_per_phase = 100;
    int num_ranks = 20;
    int repititions = 3;
    char *output_file_name = "experiment_log";
    char *input_dir = NULL;
    while ((opt = getopt(argc, argv, "n:t:p:r:o:i:")) != -1) {
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
        case 'o':
            output_file_name = optarg;
            break;
        case 'i':
            input_dir = optarg;
            break;
        default:
            printf("Unknown option: %c\n", opt);
            exit(1);
        }
    }
    // init random seed
    srand((unsigned) time(NULL));

    int num_threads = 1;
#pragma omp parallel
#pragma omp single
    num_threads = omp_get_num_threads();

    if (num_threads == 1) {
        // no need for locking
        mca_pml_ob1_matching_protection = false;
    }

    printf("Run with %d Threads\n", num_threads);

    if (implementation_list_size == 0) {
        printf("No implementations registered\n");
        exit(1);
    }

    if (input_dir) {
        read_events_from_dir(input_dir);
    } else {
        sequence_info sequence;

        sequence.ops = prepare_envelopes_random(num_phases, num_tags_per_phase, num_ranks, false);
        sequence.name = "random_no_wildcard";
        sequence.has_any_source = false;
        sequence.has_any_tag = false;
        sequence.num_phases = num_phases;
        sequence.phase_size = num_tags_per_phase*2;
        register_sequence(&sequence);

        sequence.ops = prepare_envelopes_random(num_phases, num_tags_per_phase, num_ranks, true);
        sequence.name = "random_with_wildcard";
        sequence.has_any_source = true;
        sequence.has_any_tag = true;
        sequence.num_phases = num_phases;
        sequence.phase_size = num_tags_per_phase*2;
        register_sequence(&sequence);

        sequence.ops = prepare_envelopes_randomized_phases(num_phases, num_tags_per_phase,
                                                           num_ranks, false);
        sequence.name = "random_phase_no_wildcard";
        sequence.has_any_source = false;
        sequence.has_any_tag = false;
        sequence.num_phases = num_phases;
        sequence.phase_size = num_tags_per_phase*2;
        register_sequence(&sequence);

        sequence.ops = prepare_envelopes_randomized_phases(num_phases, num_tags_per_phase,
                                                           num_ranks, true);
        sequence.name = "random_phase_with_wildcard";
        sequence.has_any_source = true;
        sequence.has_any_tag = true;
        sequence.num_phases = num_phases;
        sequence.phase_size = num_tags_per_phase*2;
        register_sequence(&sequence);

        sequence.ops = prepare_envelopes_rsend_phases(num_phases, num_tags_per_phase, num_ranks,
                                                      false);
        sequence.name = "rsend_phase_no_wildcard";
        sequence.has_any_source = false;
        sequence.has_any_tag = false;
        sequence.num_phases = num_phases;
        sequence.phase_size = num_tags_per_phase*2;
        register_sequence(&sequence);

        sequence.ops = prepare_envelopes_unexpected_phases(num_phases, num_tags_per_phase,
                                                           num_ranks, false);
        sequence.name = "unexpected_phase_no_wildcard";
        sequence.has_any_source = false;
        sequence.has_any_tag = false;
        sequence.num_phases = num_phases;
        sequence.phase_size = num_tags_per_phase*2;
        register_sequence(&sequence);

        sequence.ops = prepare_envelopes_perfect_phases(num_phases, num_tags_per_phase, num_ranks,
                                                        false);
        sequence.name = "perfect_phase_no_wildcard";
        sequence.has_any_source = false;
        sequence.has_any_tag = false;
        sequence.num_phases = num_phases;
        sequence.phase_size = num_tags_per_phase*2;
        register_sequence(&sequence);
    }

    for (int i = 0; i < repititions; ++i) {
        printf("Run %d\n", i);
        sequence_info *sequence = sequence_list_head;
        for (int seq = 0; seq < sequence_list_size; seq++) {
            assert(sequence != NULL);
            run_for_all_implementations(sequence->name, sequence->num_phases, sequence->phase_size,
                                        sequence->ops, sequence->has_any_tag,
                                        sequence->has_any_source);
            sequence = sequence->next;
        }
    }

    write_results_to_csv(output_file_name, num_threads);

    return 0;
}
