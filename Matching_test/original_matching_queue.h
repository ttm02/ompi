#ifndef OMPI_ORIGINAL_MATCHING_QUEUE_H
#define OMPI_ORIGINAL_MATCHING_QUEUE_H

#include <stdbool.h>

void* default_init_matching_queues();

void default_destroy_matching_queues(void *matching_queues);

bool default_try_match_incoming(void *matching_queue, int tag, int src, void *payload);

bool default_try_match_receive(void *matching_queue, int tag, int src, void *payload);

#endif // OMPI_ORIGINAL_MATCHING_QUEUE_H
