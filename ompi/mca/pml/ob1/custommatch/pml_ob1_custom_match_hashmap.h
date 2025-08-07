/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2018      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2018      Sandia National Laboratories.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef PML_OB1_CUSTOM_MATCH_HASHMAP_H
#define PML_OB1_CUSTOM_MATCH_HASHMAP_H

#include "../../../../../opal/include/opal/prefetch.h"
#include "../pml_ob1_recvfrag.h"
#include "../pml_ob1_recvreq.h"

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>

#ifdef NO_DEBUGGING_UNDER_PERFORMANCE_TESTING
#    undef CUSTOM_MATCH_DEBUG_VERBOSE
#    undef CUSTOM_MATCH_DEBUG
#endif

#define NUM_BUCKETS           10
#define NUM_QUEEUS_IN_BUCKETS 2
// the hash function used is expected to have one collision (peer+tag == tag+peer)

#define COUNT_COLLISIONS

typedef struct bucket_node {
    int tag;
    int peer;
    struct bucket_node *next;
    bool is_recv;
    void *value;
} bucket_node;

struct bucket {
    int tag;
    int peer;
    bucket_node *bucket_head;
    bucket_node *bucket_tail;
    bool is_recv; // can only be changed while locked
};

typedef struct bucket_collection {
    //  efficient access when no collisions are present
    struct bucket buckets[NUM_QUEEUS_IN_BUCKETS];

    // other bucket used on more collisions: need traversal and lock
    opal_mutex_t mutex; // if locking is necessary
    bucket_node *other_keys_bucket_head;
    bucket_node *other_keys_bucket_tail;
} bucket_collection;

typedef struct hashmap {
    bucket_collection buckets[NUM_BUCKETS];
    bucket_node *memory_pool;
    opal_mutex_t mutex;
#ifdef COUNT_COLLISIONS
    int num_collisions;
#endif
} hashmap;

// same name as used in other implementations
typedef hashmap custom_match_prq;

// simple hash function should suffice
// TODO evaluate other hash functions?
static inline int matching_hash_func(int tag, int peer)
{
    return (tag + peer) % NUM_BUCKETS;
}
/*
static inline int custom_match_prq_cancel(custom_match_prq *list, void *req)
{
    assert(0 && "Not implemented");
    // TODO implement
    // this is the most inefficient operation, as we need to go through all buckets
    // luckily we dont need to lock for that, as if another T matches in the mean time cancel just
    // does nothing
    return 0;
}*/

static inline void to_memory_pool(hashmap *map, bucket_node *node)
{
    OB1_MATCHING_LOCK(&map->mutex);
    node->next = map->memory_pool;
    map->memory_pool = node;
    OB1_MATCHING_UNLOCK(&map->mutex);
}

static inline bucket_node *get_bucket_node(hashmap *map)
{
    // fetch from memory pool or allocate if pool is empty
    OB1_MATCHING_LOCK(&map->mutex);
    if (map->memory_pool != NULL) {
        bucket_node *node = map->memory_pool;
        map->memory_pool = node->next;
        node->next = NULL;
        OB1_MATCHING_UNLOCK(&map->mutex);
        return node;
    } else {
        OB1_MATCHING_UNLOCK(&map->mutex);
        return calloc(1, sizeof(bucket_node));
    }
}

static inline void custom_match_prq_cancel(hashmap* map, void* payload)
{
    assert(0 && "Not implemented");
    //TODO implement
    // most costly operation: need to search all buckets ontul found element or everything was searched
}

// Notes for a lock-free design:
// problem: lock-free linked list require more effort e.g. an extra marker to mark node as invalid,
// otherwise, other T can modify the node whie we are removing it even besser solution: encode
// is_recv in first bit of ptr, as than it is actually part of the CAS if one wants to be 100%
// secure: the get_memory checks if malloc returns something where first bit of actual ptr is 0

static inline void insert_to_list(struct bucket *my_bucket, bucket_node *new_elem, bool is_recv)
{
    assert(new_elem->next == NULL);
    if (my_bucket->bucket_head == NULL || my_bucket->bucket_tail == NULL) {
        // on empty list
        my_bucket->bucket_tail = new_elem;
        my_bucket->bucket_head = new_elem;
        my_bucket->is_recv = is_recv; // update list status
    } else {
        // list has at least one element
        assert(my_bucket->bucket_tail->next == NULL);
        assert(my_bucket->is_recv == is_recv);
        my_bucket->bucket_tail->next = new_elem;
        my_bucket->bucket_tail = new_elem;
    }
}
static inline void *remove_from_list(struct bucket *my_bucket)
{
    bucket_node *elem_to_dequeue = my_bucket->bucket_head;
    assert(elem_to_dequeue != NULL);
    my_bucket->bucket_head = elem_to_dequeue->next;
    if (elem_to_dequeue->next == NULL) {
        // removal of last element
        my_bucket->bucket_tail = NULL;
    }
    return elem_to_dequeue;
}

// TODO can i force the compiler to instantiate a "templated" version where is_recv is template
// parameter?

// returns the match (and removed matched from queue)
// or inserts into the queue if no match and returns void
// basically combining the different matching queues
static inline void *get_match_or_insert(hashmap *map, int tag, int peer, void *payload, bool is_recv)
{
    //printf("access bucket %d (%d,%d,%d)\n",hash_func(tag, peer),tag,peer,is_recv);
    bucket_collection *my_bucket = &map->buckets[matching_hash_func(tag, peer)];
    OB1_MATCHING_LOCK(&my_bucket->mutex);

    for (int i = 0; i < NUM_QUEEUS_IN_BUCKETS; ++i) {
        if (OPAL_UNLIKELY(my_bucket->buckets[i].tag == -1)) {
            // initialize on first use
            my_bucket->buckets[i].tag = tag;
            my_bucket->buckets[i].peer = peer;
        }
        if (OPAL_LIKELY(my_bucket->buckets[i].tag == tag && my_bucket->buckets[i].peer == peer)) {
            // found correct bucket

            // if list empty or same mode: insert to queue
            if (my_bucket->buckets[i].is_recv == is_recv
                || my_bucket->buckets[i].bucket_head == NULL) {
                bucket_node *new_elem = get_bucket_node(map);
                new_elem->tag = tag;
                new_elem->peer = peer;
                new_elem->next = NULL;
                new_elem->is_recv = is_recv;
                new_elem->value = payload;
                insert_to_list(&my_bucket->buckets[i], new_elem, is_recv);
                OB1_MATCHING_UNLOCK(&my_bucket->mutex);
                return NULL; // inserted into queue without a match

            } else {
                // not empty and holds the other queue
                // dequeue matching element
                bucket_node *elem_to_dequeue = remove_from_list(&my_bucket->buckets[i]);
                OB1_MATCHING_UNLOCK(&my_bucket->mutex);
                // free element
                void *retval = elem_to_dequeue->value;
                to_memory_pool(map, elem_to_dequeue);

                return retval;
            }
        }
    }
    // multiple hash collisions
#ifdef COUNT_COLLISIONS
    __atomic_add_fetch(&map->num_collisions,1,__ATOMIC_ACQ_REL);
#endif
    bucket_node *prev_elem = NULL;
    bucket_node *elem = my_bucket->other_keys_bucket_head;

    while (elem != NULL) {
        if (elem->tag == tag && elem->peer == peer) {
            // found matching entry
            if (elem->is_recv == is_recv) {
                // same queue: insert at end
                bucket_node *new_elem = get_bucket_node(map);
                new_elem->tag = tag;
                new_elem->peer = peer;
                new_elem->next = NULL;
                new_elem->is_recv = is_recv;
                new_elem->value = payload;
                my_bucket->other_keys_bucket_tail->next = new_elem;
                my_bucket->other_keys_bucket_tail = new_elem;
                OB1_MATCHING_UNLOCK(&my_bucket->mutex);
                return NULL;
            } else {
                // match: dequeue
                if (prev_elem != NULL) {
                    prev_elem->next = elem->next;
                } else {
                    // first list elem
                    my_bucket->other_keys_bucket_head = elem->next;
                }
                // last elem
                if (my_bucket->other_keys_bucket_tail == elem) {
                    my_bucket->other_keys_bucket_tail = prev_elem;
                    // also works when list is emptied
                }
                void *retval = elem->value;
                to_memory_pool(map, elem);
                OB1_MATCHING_UNLOCK(&my_bucket->mutex);
                return retval;
            }
        }
        prev_elem = elem;
        elem = prev_elem->next;
    }
    // mo match found: insert at end (or on empty list)
    bucket_node *new_elem = get_bucket_node(map);
    new_elem->tag = tag;
    new_elem->peer = peer;
    new_elem->next = NULL;
    new_elem->is_recv = is_recv;
    new_elem->value = payload;

    if (my_bucket->other_keys_bucket_tail == NULL) {
        assert(my_bucket->other_keys_bucket_head == NULL);
        my_bucket->other_keys_bucket_head = new_elem;
        my_bucket->other_keys_bucket_tail = new_elem;
    } else {
        my_bucket->other_keys_bucket_tail->next = new_elem;
        my_bucket->other_keys_bucket_tail = new_elem;
    }
    OB1_MATCHING_UNLOCK(&my_bucket->mutex);
    return NULL;
}

static inline hashmap *match_map_init(void)
{
    hashmap *map = calloc(sizeof(hashmap), 1);

    // initialize the locks
    OBJ_CONSTRUCT(&map->mutex, opal_mutex_t);
    for (int i = 0; i < NUM_BUCKETS; ++i) {
        OBJ_CONSTRUCT(&map->buckets[i].mutex, opal_mutex_t);
        for (int j = 0; j < NUM_QUEEUS_IN_BUCKETS; ++j) {
            map->buckets[i].buckets[j].tag = -1;
        }
    }
    return map;
}

static inline void match_map_destroy(hashmap *map)
{
#ifdef COUNT_COLLISIONS
    printf("Number of hash Collisions:%d\n", map->num_collisions);
#endif
    OBJ_DESTRUCT(&map->mutex);
    for (int i = 0; i < NUM_BUCKETS; ++i) {
        OBJ_DESTRUCT(&map->buckets[i].mutex);
        for (int j = 0; j < NUM_QUEEUS_IN_BUCKETS; ++j) {
            bucket_node *elem = map->buckets[i].buckets[j].bucket_head;
            while (elem != NULL) {
                bucket_node *next_elem = elem->next;
                free(elem);
                elem = next_elem;
            }
        }
    }
    bucket_node *elem = map->memory_pool;
    while (elem != NULL) {
        bucket_node *next_elem = elem->next;
        free(elem);
        elem = next_elem;
    }
    free(map);
}

#endif
