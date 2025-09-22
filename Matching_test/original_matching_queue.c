
#include <stdbool.h>
#include <stdlib.h>

#include "original_matching_queue.h"

// forward declare to remove problem with include ordering when including this internal header
struct custom_match_prq;
struct custom_match_umq;
typedef struct custom_match_prq custom_match_prq;
typedef struct custom_match_umq custom_match_umq;

#define NO_DEBUGGING_UNDER_PERFORMANCE_TESTING

#include "../ompi/mca/pml/ob1/custommatch/pml_ob1_custom_match_linkedlist.h" // the default one
// #include "../ompi/mca/pml/ob1/custommatch/pml_ob1_custom_match_arrays.h"

typedef struct matching_data {
    custom_match_prq *pq;
    custom_match_umq *uq;
} matching_data;

void *default_init_matching_queues()
{
    matching_data *matching_queues = malloc(sizeof(matching_data));
    matching_queues->pq = custom_match_prq_init();
    matching_queues->uq = custom_match_umq_init();
    return (void *) matching_queues;
}

void default_destroy_matching_queues(void *matching_queues)
{
    custom_match_prq_destroy(((matching_data *) matching_queues)->pq);
    custom_match_umq_destroy(((matching_data *) matching_queues)->uq);
    free(matching_queues);
}

bool default_try_match_incoming(void *matching_queues, int tag, int src, void *payload)
{
    bool retval = true;
    // openmp critical instead of the OB1 Matching lock, as we dont have a communicator object in
    // this context
#pragma omp critical

    {
        void *recv_req = custom_match_prq_find_dequeue_verify(((matching_data *) matching_queues)
                                                                  ->pq,
                                                              tag, src);
        if (recv_req) {
            retval = true;
        } else {
            // no match => enqueue to unexpected queue
            custom_match_umq_append(((matching_data *) matching_queues)->uq, tag, src, payload);
            retval = false;
        }
    }
    return retval;
}

bool default_try_match_receive(void *matching_queues, int tag, int src, void *payload)
{
    bool retval = true;
    // openmp critical instead of the OB1 Matching lock, as we dont have a communicator object in
    // this context
#pragma omp critical
    {
        custom_match_umq_node *hold_prev;
        custom_match_umq_node *hold_elem;
        int hold_index;
        void *msg_found = custom_match_umq_find_verify_hold(((matching_data *) matching_queues)->uq,
                                                            tag, src, &hold_prev, &hold_elem,
                                                            &hold_index);

        if (msg_found) {
            custom_match_umq_remove_hold(((matching_data *) matching_queues)->uq, hold_prev,
                                         hold_elem,
                                         hold_index); // actually remove
            retval = true;
        } else {
            // not found => post receive into PRQ
            custom_match_prq_append(((matching_data *) matching_queues)->pq, payload, tag, src);
            retval = false;
        }
    }
    return retval;
}
