
#include <stdbool.h>
#include <stdlib.h>
#include "hashmap_matching_queue_overtake_wildcard.h"

#define OUTSIDE_CONFIGURATION
#define NO_DEBUGGING_UNDER_PERFORMANCE_TESTING
#define WILDCARD_SUPPORT


#include "../ompi/mca/pml/ob1/custommatch/pml_ob1_custom_match_hashmap.h"

void *hashmap_overtake_wild_init_matching_queues()
{
    return (void*)match_map_init();
}

void hashmap_overtake_wild_destroy_matching_queues(void *matching_queues)
{
    match_map_destroy((hashmap*)matching_queues);
}

bool hashmap_overtake_wild_try_match_incoming(void *matching_queue, int tag, int src, void *payload)
{
    bool retval = true;
    void **to_fill = NULL;
    void *recv_req = get_match_or_insert((hashmap*)matching_queue, tag, src, &to_fill, false);
    if (recv_req) {
        retval = true;
    } else {
        // no match => enqueue to unexpected queue
        __atomic_store_n(to_fill, payload, __ATOMIC_RELAXED);
        retval = false;
    }

    return retval;
}

bool hashmap_overtake_wild_try_match_receive(void *matching_queue, int tag, int src, void *payload)
{
    bool retval = true;
    void **to_fill = NULL;

    void *msg_found = get_match_or_insert((hashmap*)matching_queue, tag, src, &to_fill, true);

    if (msg_found) {

        retval = true;
    } else {
        __atomic_store_n(to_fill, payload, __ATOMIC_RELAXED);
        retval = false;
    }

    return retval;
}
