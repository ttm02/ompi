

#ifndef HASHMAP_MATCHING_QUEUE_H
#define HASHMAP_MATCHING_QUEUE_H
#include <stdbool.h>
#include <stdlib.h>

// switch on mpi internal locking
bool mca_pml_ob1_matching_protection = true;

#define NO_DEBUGGING_UNDER_PERFORMANCE_TESTING

#include "../ompi/mca/pml/ob1/custommatch/pml_ob1_custom_match_hashmap.h"

typedef hashmap matching_data;

static inline matching_data *init_matching_queues()
{
    return match_map_init();
}

static inline void destroy_matching_queues(matching_data *matching_queues)
{
    match_map_destroy(matching_queues);
}

static inline bool try_match_incoming(matching_data *matching_queue, int tag, int src, void *payload)
{
    bool retval = true;
    void **to_fill = NULL;
    void *recv_req = get_match_or_insert(matching_queue, tag, src, &to_fill, false);
    if (recv_req) {
        retval = true;
    } else {
        // no match => enqueue to unexpected queue
        __atomic_store_n(to_fill, payload, __ATOMIC_RELAXED);
        retval = false;
    }

    return retval;
}

static inline bool try_match_receive(matching_data *matching_queue, int tag, int src, void *payload)
{
    bool retval = true;
    void **to_fill = NULL;

    void *msg_found = get_match_or_insert(matching_queue, tag, src, &to_fill, true);

    if (msg_found) {

        retval = true;
    } else {
        __atomic_store_n(to_fill, payload, __ATOMIC_RELAXED);
        retval = false;
    }

    return retval;
}

#endif // HASHMAP_MATCHING_QUEUE_H
