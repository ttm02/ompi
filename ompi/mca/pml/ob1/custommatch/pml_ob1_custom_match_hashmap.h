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
#include "../pml_ob1.h"
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
    int mask = 0x7FFFFFFF; // only sign bit not set
    // tag may be negative on some internal communication
    return ((tag& mask) + peer) % NUM_BUCKETS;
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

static inline void* to_memory_pool(hashmap *map, bucket_node *node)
{
    void* retval = __atomic_load_n(&node->value,__ATOMIC_RELAXED);
    while (NULL == retval) {
        // wait until other thread has finished initializing this value
        retval=__atomic_load_n(&node->value,__ATOMIC_ACQUIRE);
    }
    __atomic_store_n(&node->value,NULL,__ATOMIC_RELEASE);
    OB1_MATCHING_LOCK(&map->mutex);
    node->next = map->memory_pool;
    map->memory_pool = node;
    OB1_MATCHING_UNLOCK(&map->mutex);
    return retval;
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
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("custom_match_prq_cancel - list: %p req: %p\n", map, payload);
#endif

    // most costly operation: need to search all buckets until found element or everything was
    // searched
    for (int i = 0; i < NUM_BUCKETS; ++i) {
        bucket_collection *my_bucket = &map->buckets[i];
        OB1_MATCHING_LOCK(&my_bucket->mutex);
        for (int j = 0; j < NUM_QUEEUS_IN_BUCKETS; ++j) {
            bucket_node* prev_elem=NULL;
            bucket_node* elem = my_bucket->buckets[j].bucket_head;
            while (elem!=NULL) {
                if (elem->value==payload) {
                    // found elem
                    if (prev_elem==NULL) {
                        my_bucket->buckets[j].bucket_head = elem->next;
                    }else {
                        prev_elem->next=elem->next;
                    }
                        if (elem->next == NULL) {
                            // removal of last element
                            my_bucket->buckets[j].bucket_tail = NULL;
                        }
                    OB1_MATCHING_UNLOCK(&my_bucket->mutex);
#if CUSTOM_MATCH_DEBUG_VERBOSE
                    printf("custom_match_prq_cancel - cancelled (%d,%d) list: %p req: %p\n", elem->tag,elem->peer,map, payload);
#endif

                    to_memory_pool(map, elem);
                    return;
                }
                prev_elem = elem;
                elem = prev_elem->next;
            }
        }


        bucket_node* prev_elem=NULL;
        bucket_node* elem = my_bucket->other_keys_bucket_head;
        while (elem!=NULL) {
            if (elem->value==payload) {
                // found elem
                if (prev_elem==NULL) {
                    my_bucket->other_keys_bucket_head = elem->next;
                }else {
                    prev_elem->next=elem->next;
                }
                if (elem->next == NULL) {
                    // removal of last element
                    my_bucket->other_keys_bucket_tail = NULL;
                }
                OB1_MATCHING_UNLOCK(&my_bucket->mutex);
#if CUSTOM_MATCH_DEBUG_VERBOSE
                printf("custom_match_prq_cancel - cancelled (%d,%d) list: %p req: %p\n", elem->tag,elem->peer,map, payload);
#endif

                to_memory_pool(map, elem);
                return;
            }
            prev_elem = elem;
            elem = prev_elem->next;
        }
        // not in this bucket
        OB1_MATCHING_UNLOCK(&my_bucket->mutex);
    }
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("custom_match_prq_cancel - not in list anymore list: %p req: %p\n", map, payload);
#endif


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
// to_fill will be set to void** where teh actual payload data needs to be dropped, if elem is inserted
static inline void *get_match_or_insert(hashmap *map, int tag, int peer, void*** to_fill, bool is_recv)
{
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("%s try match (%d,%d)\n",is_recv?"recv posted":"msg arrived",tag,peer);
#endif
    //printf("access bucket %d (%d,%d,%d)\n",matching_hash_func(tag, peer),tag,peer,is_recv);
    bucket_collection *my_bucket = &map->buckets[matching_hash_func(tag, peer)];
    OB1_MATCHING_LOCK(&my_bucket->mutex);

    for (int i = 0; i < NUM_QUEEUS_IN_BUCKETS; ++i) {
        if (OPAL_UNLIKELY(my_bucket->buckets[i].tag == -1)) {
            // initialize on first use
            my_bucket->buckets[i].tag = tag;
            my_bucket->buckets[i].peer = peer;
#if CUSTOM_MATCH_DEBUG_VERBOSE
            printf("initialize bucket %d_%d: (%d,%d)\n",matching_hash_func(tag, peer),i,tag,peer);
#endif
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
                assert(__atomic_load_n(&new_elem->value,__ATOMIC_RELAXED)==NULL);
                *to_fill = &new_elem->value;
                insert_to_list(&my_bucket->buckets[i], new_elem, is_recv);
                OB1_MATCHING_UNLOCK(&my_bucket->mutex);
#if CUSTOM_MATCH_DEBUG_VERBOSE
                printf("add (%d,%d) to %s \n",tag,peer, is_recv?"prq":"umq");
#endif
                return NULL; // inserted into queue without a match

            } else {
                // not empty and holds the other queue
                // dequeue matching element
                bucket_node *elem_to_dequeue = remove_from_list(&my_bucket->buckets[i]);
                OB1_MATCHING_UNLOCK(&my_bucket->mutex);
#if CUSTOM_MATCH_DEBUG_VERBOSE
                printf("matched (%d,%d) from %s \n",tag,peer, !is_recv?"prq":"umq");
#endif
                // free element
                return to_memory_pool(map, elem_to_dequeue);
            }
        }
    }
    // multiple hash collisions
#ifdef COUNT_COLLISIONS
    __atomic_add_fetch(&map->num_collisions,1,__ATOMIC_ACQ_REL);
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("collision in bucket %d (%d,%d)\n",matching_hash_func(tag, peer),tag,peer);
#endif
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
                assert(__atomic_load_n(&new_elem->value,__ATOMIC_RELAXED)==NULL);
                *to_fill = &new_elem->value;
                my_bucket->other_keys_bucket_tail->next = new_elem;
                my_bucket->other_keys_bucket_tail = new_elem;
                OB1_MATCHING_UNLOCK(&my_bucket->mutex);
#if CUSTOM_MATCH_DEBUG_VERBOSE
                printf("add (%d,%d) to %s \n",tag,peer, is_recv?"prq":"umq");
#endif
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
                OB1_MATCHING_UNLOCK(&my_bucket->mutex);

#if CUSTOM_MATCH_DEBUG_VERBOSE
                printf("matched (%d,%d) from %s \n",tag,peer, !is_recv?"prq":"umq");
#endif

                return to_memory_pool(map, elem);
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
    assert(__atomic_load_n(&new_elem->value,__ATOMIC_RELAXED)==NULL);
    *to_fill = &new_elem->value;

    if (my_bucket->other_keys_bucket_tail == NULL) {
        assert(my_bucket->other_keys_bucket_head == NULL);
        my_bucket->other_keys_bucket_head = new_elem;
        my_bucket->other_keys_bucket_tail = new_elem;
    } else {
        my_bucket->other_keys_bucket_tail->next = new_elem;
        my_bucket->other_keys_bucket_tail = new_elem;
    }
    OB1_MATCHING_UNLOCK(&my_bucket->mutex);
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("add (%d,%d) to %s \n",tag,peer, is_recv?"prq":"umq");
#endif
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
