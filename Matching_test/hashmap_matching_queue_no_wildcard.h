#ifndef HASHMAP_MATCHING_QUEUE_NO_WILDCARD_H
#define HASHMAP_MATCHING_QUEUE_NO_WILDCARD_H

#include <stdbool.h>

void * hashmap_no_wild_init_matching_queues();

void hashmap_no_wild_destroy_matching_queues(void *matching_queues);

bool hashmap_no_wild_try_match_incoming(void *matching_queue, int tag, int src, void *payload);

bool hashmap_no_wild_try_match_receive(void *matching_queue, int tag, int src, void *payload);

#endif // HASHMAP_MATCHING_QUEUE_NO_WILDCARD_H
