#ifndef HASHMAP_OVERTAKE_WILDCARD_MATCHING_QUEUE_H
#define HASHMAP_OVERTAKE_WILDCARD_MATCHING_QUEUE_H

#include <stdbool.h>

void * hashmap_overtake_wild_init_matching_queues();

void hashmap_overtake_wild_destroy_matching_queues(void *matching_queues);

bool hashmap_overtake_wild_try_match_incoming(void *matching_queue, int tag, int src, void *payload);

bool hashmap_overtake_wild_try_match_receive(void *matching_queue, int tag, int src, void *payload);

#endif // HASHMAP_OVERTAKE_WILDCARD_MATCHING_QUEUE_H
