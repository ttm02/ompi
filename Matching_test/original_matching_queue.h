

#ifndef OMPI_ORIGINAL_MATCHING_QUEUE_H
#define OMPI_ORIGINAL_MATCHING_QUEUE_H
#include <stdbool.h>
#include <stdlib.h>

// forward declare to remove problem with include ordering when including this internal header
struct custom_match_prq;
struct custom_match_umq;
typedef struct custom_match_prq custom_match_prq;
typedef struct custom_match_umq custom_match_umq;
// switch on mpi internal locking
bool mca_pml_ob1_matching_protection = true;

#define NO_DEBUGGING_UNDER_PERFORMANCE_TESTING

#include "../ompi/mca/pml/ob1/custommatch/pml_ob1_custom_match_linkedlist.h"
// #include "../ompi/mca/pml/ob1/custommatch/pml_ob1_custom_match_arrays.h"

typedef struct matching_data {
    custom_match_prq *pq;
    custom_match_umq *uq;
} matching_data;

static inline matching_data *init_matching_queues()
{
    matching_data *matching_queues = malloc(sizeof(matching_data));
    matching_queues->pq = custom_match_prq_init();
    matching_queues->uq = custom_match_umq_init();
    return matching_queues;
}

static inline void destroy_matching_queues(matching_data *matching_queues)
{
    custom_match_prq_destroy(matching_queues->pq);
    custom_match_umq_destroy(matching_queues->uq);
    free(matching_queues);
}

static inline bool try_match_incoming(matching_data *matching_queue, int tag, int src, void *payload)
{
    bool retval = true;
#pragma omp critical
    {
        void *recv_req = custom_match_prq_find_dequeue_verify(matching_queue->pq, tag, src);
        if (recv_req) {
            retval = true;
        } else {
            // no match => enqueue to unexpected queue
            custom_match_umq_append(matching_queue->uq, tag, src, payload);
            retval = false;
        }
    }
    return retval;
}

static inline bool try_match_receive(matching_data *matching_queue, int tag, int src, void *payload)
{
    bool retval = true;
#pragma omp critical
    {
        custom_match_umq_node *hold_prev;
        custom_match_umq_node *hold_elem;
        int hold_index;
        void *msg_found = custom_match_umq_find_verify_hold(matching_queue->uq, tag, src,
                                                            &hold_prev, &hold_elem, &hold_index);

        if (msg_found) {
            custom_match_umq_remove_hold(matching_queue->uq, hold_prev, hold_elem,
                                         hold_index); // actually remove
            retval = true;
        } else {
            // not found => post receive into PRQ
            custom_match_prq_append(matching_queue->pq, payload, tag, src);
            retval = false;
        }
    }
    return retval;
}

#endif // OMPI_ORIGINAL_MATCHING_QUEUE_H
