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
#include "../../pml_constants.h"
#include "../pml_ob1.h"
#include "../pml_ob1_recvfrag.h"
#include "../pml_ob1_recvreq.h"

#include <assert.h>
#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdint.h>

//TODO: reduce the amount of duplicate code when traversing the buckets!!

#ifdef NO_DEBUGGING_UNDER_PERFORMANCE_TESTING
#    undef CUSTOM_MATCH_DEBUG_VERBOSE
#    undef CUSTOM_MATCH_DEBUG
#endif

// to avoid false sharing of buckets
#define LEVEL1_DCACHE_LINESIZE 64
//getconf LEVEL1_DCACHE_LINESIZE

#define NUM_BUCKETS           16
#define NUM_QUEEUS_IN_BUCKETS 2
// the hash function used is expected to have one collision (peer+tag == tag+peer)

#define COUNT_COLLISIONS

#ifndef OUTSIDE_CONFIGURATION
#define WILDCARD_SUPPORT
#define WILDCARD_NO_OVERTAKE_SUPPORT
#define BRANCHLESS_BUCKET_SELECTOR
#define ADAPT_BUCKETS
#endif


#ifdef WILDCARD_NO_OVERTAKE_SUPPORT
#ifndef WILDCARD_SUPPORT
_Static_assert(0 && "Erroneous Configuration\n");
#endif
#endif

typedef struct bucket_node {
    int tag;
    int peer;
    struct bucket_node *next;
    bool is_recv;
    void *value;
#ifdef WILDCARD_NO_OVERTAKE_SUPPORT
    int seq_num;
#endif
} bucket_node;


// info may be shared among threads and is not changed often
struct bucket_info {
    int tag;
    int peer;
};

// this will be updated by different threads
struct bucket {
    bucket_node *bucket_head;
    bucket_node *bucket_tail;
    bool is_recv; // can only be changed while locked
};

typedef struct bucket_collection {
    //  efficient access when no collisions are present
    struct bucket_info bucket_infos[NUM_QUEEUS_IN_BUCKETS];
    //TODO make sure the cache line ends here??
    opal_mutex_t mutex; // if locking is necessary
    struct bucket buckets[NUM_QUEEUS_IN_BUCKETS];
    // other bucket used on more collisions: need traversal and lock
    struct bucket overflow_bucket;
    //TODO align to the cache line??
} bucket_collection;

typedef struct hashmap {
#ifdef WILDCARD_SUPPORT
    struct bucket wildcard_bucket;
    opal_mutex_t wildcard_mutex; // guarding the wildcard bucket
#ifdef WILDCARD_NO_OVERTAKE_SUPPORT
    int seq_num;
#endif
    //TODO make sure the cache line ends here??
#endif

    bucket_collection buckets[NUM_BUCKETS];
    //TODO make sure the cache line ends here??
    bucket_node *memory_pool;
    opal_mutex_t mutex;// guarding the memory pool
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

static inline void* to_memory_pool(hashmap *map, bucket_node *node)
{
    void* retval = __atomic_load_n(&node->value,__ATOMIC_ACQUIRE);
    while (OPAL_UNLIKELY(NULL == retval)) {
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
        bucket_node* elem = my_bucket->overflow_bucket.bucket_head;
        while (elem!=NULL) {
            if (elem->value==payload) {
                // found elem
                if (prev_elem==NULL) {
                    my_bucket->overflow_bucket.bucket_head = elem->next;
                }else {
                    prev_elem->next=elem->next;
                }
                if (elem->next == NULL) {
                    // removal of last element
                    my_bucket->overflow_bucket.bucket_tail = NULL;
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
// otherwise, other T can modify the node while we are removing it even besser solution: encode
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

#ifdef WILDCARD_SUPPORT
#ifdef WILDCARD_NO_OVERTAKE_SUPPORT
// integer wrap around logical clock where current is now
// meaning current+1 is oldest possible while current is the newest possible
static inline bool is_older(int a, int b, int current)
{
    if (a <= current && b > current) {
        return false;
    }
    if (a > current && b <= current) {
        return true;
    }
    assert((a<=current && b<=current) || (a>current && b>current));
    // end wrap around handling
    return a < b;
}

static inline void *try_match_from_wildcard_prq(hashmap *map, int tag, int peer, void*** to_fill)
{
    // wildcard bucket: need  lock
    OB1_MATCHING_LOCK(&map->wildcard_mutex);
// need to check if msg in normal bucket is older
    bucket_collection *my_bucket = &map->buckets[matching_hash_func(tag, peer)];
//    OB1_MATCHING_LOCK(&my_bucket->mutex);

    // other threads may change this, but as we locked all relevant buckets, that's not a problem
    int current_seq = __atomic_load_n(&map->seq_num,__ATOMIC_RELAXED);

    // traverse wildcard bucket
    bucket_node* in_wildcard=NULL;

        // the wildcard bucket can only hold posted recvs as unexpected msg cannot have a wildcard
        bucket_node* elem = map->wildcard_bucket.bucket_head;
        while (elem!=NULL) {
            if ((elem->tag == OMPI_ANY_TAG || elem->tag == tag)
                &&(elem->peer == OMPI_ANY_SOURCE || elem->peer == peer)) {
                // match
                in_wildcard = elem;
                break;
                }
            elem = elem->next;
        }

    // in normal bucket
    for (int i = 0; i < NUM_QUEEUS_IN_BUCKETS; ++i) {
        if (OPAL_UNLIKELY(my_bucket->bucket_infos[i].tag == -1)) {
            // initialize on first use
            my_bucket->bucket_infos[i].tag = tag;
            my_bucket->bucket_infos[i].peer = peer;
#if CUSTOM_MATCH_DEBUG_VERBOSE
            printf("initialize bucket %d_%d: (%d,%d)\n",matching_hash_func(tag, peer),i,tag,peer);
#endif
        }
        if (OPAL_LIKELY(my_bucket->bucket_infos[i].tag == tag && my_bucket->bucket_infos[i].peer == peer)) {
            // found correct bucket
            // if list empty or same mode: insert to queue
            if (my_bucket->buckets[i].is_recv == false
                || my_bucket->buckets[i].bucket_head == NULL) {

                if (in_wildcard) {
                    // match to wildcard
                    goto match_to_wildcard;
                }else {
                    // no match: insert to UMQ
                    bucket_node *new_elem = get_bucket_node(map);
                    new_elem->tag = tag;
                    new_elem->peer = peer;
                    new_elem->next = NULL;
                    new_elem->seq_num=__atomic_add_fetch(&map->seq_num,1,__ATOMIC_RELAXED);
                    new_elem->is_recv = false;
                    assert(__atomic_load_n(&new_elem->value,__ATOMIC_RELAXED)==NULL);
                    *to_fill = &new_elem->value;
                    insert_to_list(&my_bucket->buckets[i], new_elem, false);
                    OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
  //                  OB1_MATCHING_UNLOCK(&my_bucket->mutex);
                    return NULL;
                }
                }else {
                    // match to older one
                    if (in_wildcard && is_older(in_wildcard->seq_num,elem->seq_num,current_seq)) {
                        goto match_to_wildcard;
                    }else {
                        bucket_node *to_remove= remove_from_list(&my_bucket->buckets[i]);
                        OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
    //                    OB1_MATCHING_UNLOCK(&my_bucket->mutex);
                        return to_remove;
                    }
                }
        }
    }
    // overflow bucket
        bucket_node *prev_elem = NULL;
    elem = my_bucket->overflow_bucket.bucket_head;

    while (elem != NULL) {
        if (elem->tag == tag && elem->peer == peer) {
            // found matching entry
            if (elem->is_recv == false) {
                // is in opposite queue
                if (in_wildcard) {
                    goto match_to_wildcard;
                }
                // same queue: insert at end
                bucket_node *new_elem = get_bucket_node(map);
                new_elem->tag = tag;
                new_elem->peer = peer;
                new_elem->next = NULL;
                new_elem->seq_num=__atomic_add_fetch(&map->seq_num,1,__ATOMIC_RELAXED);
                new_elem->is_recv = false;
                assert(__atomic_load_n(&new_elem->value,__ATOMIC_RELAXED)==NULL);
                *to_fill = &new_elem->value;
                my_bucket->overflow_bucket.bucket_tail->next = new_elem;
                my_bucket->overflow_bucket.bucket_tail = new_elem;
#if CUSTOM_MATCH_DEBUG_VERBOSE
                printf("add (%d,%d) to %s \n",tag,peer, false?"prq":"umq");
#endif
                OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
      //          OB1_MATCHING_UNLOCK(&my_bucket->mutex);
                return NULL;
            } else {
                if (in_wildcard && is_older(in_wildcard->seq_num,elem->seq_num,current_seq)) {
                    goto match_to_wildcard;
                }
                // match: dequeue
                if (prev_elem != NULL) {
                    prev_elem->next = elem->next;
                } else {
                    // first list elem
                    my_bucket->overflow_bucket.bucket_head = elem->next;
                }
                // last elem
                if (my_bucket->overflow_bucket.bucket_tail == elem) {
                    my_bucket->overflow_bucket.bucket_tail = prev_elem;
                    // also works when list is emptied
                }
#if CUSTOM_MATCH_DEBUG_VERBOSE
                printf("matched (%d,%d) from %s \n",tag,peer, !false?"prq":"umq");
#endif
                OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
//                OB1_MATCHING_UNLOCK(&my_bucket->mutex);
                return elem;
            }
        }
        prev_elem = elem;
        elem = prev_elem->next;
    }
    // none found
    if (in_wildcard) {
        goto match_to_wildcard;
    }
    //  no match: insert to overflow
    bucket_node *new_elem = get_bucket_node(map);
    new_elem->tag = tag;
    new_elem->peer = peer;
    new_elem->next = NULL;
#ifdef WILDCARD_NO_OVERTAKE_SUPPORT
    new_elem->seq_num=__atomic_add_fetch(&map->seq_num,1,__ATOMIC_RELAXED);
#endif
    new_elem->is_recv = false;
    assert(__atomic_load_n(&new_elem->value,__ATOMIC_RELAXED)==NULL);
    *to_fill = &new_elem->value;

    if (my_bucket->overflow_bucket.bucket_tail == NULL) {
        assert(my_bucket->overflow_bucket.bucket_head == NULL);
        my_bucket->overflow_bucket.bucket_head = new_elem;
        my_bucket->overflow_bucket.bucket_tail = new_elem;
    } else {
        my_bucket->overflow_bucket.bucket_tail->next = new_elem;
        my_bucket->overflow_bucket.bucket_tail = new_elem;
    }
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("add (%d,%d) to %s \n",tag,peer, false?"prq":"umq");
#endif
    OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
    //OB1_MATCHING_UNLOCK(&my_bucket->mutex);
    return NULL;


    match_to_wildcard:
    elem = map->wildcard_bucket.bucket_head;
    prev_elem = NULL;
    while (elem != NULL) {
        if (elem== in_wildcard) {
            if (prev_elem) {
                prev_elem->next = elem->next;
            }else {
                map->wildcard_bucket.bucket_head = elem->next;
            }
            if (elem->next==NULL) {
                // removed last elem
                map->wildcard_bucket.bucket_head= prev_elem;
            }
            OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
            //OB1_MATCHING_UNLOCK(&my_bucket->mutex);
            return elem;
        }
        prev_elem = elem;
        elem = elem->next;
    }

    assert(false);// unreachable
}
#else
static inline void *try_match_from_wildcard_prq(hashmap *map, int tag, int peer, void*** to_fill)
{
    // wildcard bucket: need  lock
    OB1_MATCHING_LOCK(&map->wildcard_mutex);

    // traverse wildcard bucket

    // the wildcard bucket can only hold posted recvs as unexpected msg cannot have a wildcard
    bucket_node* elem = map->wildcard_bucket.bucket_head;
    bucket_node* prev = NULL;
    while (elem!=NULL) {
        if ((elem->tag == OMPI_ANY_TAG || elem->tag == tag)
            &&(elem->peer == OMPI_ANY_SOURCE || elem->peer == peer)) {
            // match: remove from list
            if (prev==NULL) {
                map->wildcard_bucket.bucket_head=elem->next;
            }else {
                prev->next=elem->next;
            }
            // update tail
            if (elem->next == NULL) {
                map->wildcard_bucket.bucket_tail=prev;
            }
            OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
            return elem;
            }
        prev = elem;
        elem = elem->next;
    }

    OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
    return NULL; // no match in wildcard bucket - continue normal matching process
}
#endif

#ifdef WILDCARD_NO_OVERTAKE_SUPPORT

static inline void *match_with_wildcard(hashmap *map, int tag, int peer, void*** to_fill)
{

    //lock ALL buckets
    OB1_MATCHING_LOCK(&map->wildcard_mutex);
    for (int i =0;i<NUM_BUCKETS;++i) {
        OB1_MATCHING_LOCK(&map->buckets[i].mutex);
    }

    bucket_node* current_oldest=NULL;
    bucket_collection* oldest_bucket=NULL;
    int current_seq_num = map->seq_num; // no need for atomic access, as we have the full lock

    // traverse all buckets to find matching

    for (int i = 0; i < NUM_BUCKETS; ++i) {
        bucket_collection *my_bucket = &map->buckets[i];
        for (int j = 0; j < NUM_QUEEUS_IN_BUCKETS; ++j) {
            if (!my_bucket->buckets[j].is_recv) {
                // if bucket holds UMQ
                bucket_node* prev_elem=NULL;
                bucket_node* elem = my_bucket->buckets[j].bucket_head;
                while (elem!=NULL) {
                    if ((peer==OMPI_ANY_SOURCE || elem->peer == peer) &&(tag==OMPI_ANY_TAG || elem->tag == tag)) {
                        // found elem
                        if (!current_oldest) {
                            current_oldest = elem;
                            oldest_bucket = my_bucket;
                        }
                        if (is_older(elem->seq_num,current_oldest->seq_num,current_seq_num)){
                            current_oldest = elem;
                            oldest_bucket = my_bucket;
                        }
                    }
                    prev_elem = elem;
                    elem = prev_elem->next;
                }
            }
        }
        //overflow bucket
        bucket_node* prev_elem=NULL;
        bucket_node* elem = my_bucket->overflow_bucket.bucket_head;
        while (elem!=NULL) {
            if (!elem->is_recv && (peer==OMPI_ANY_SOURCE || elem->peer == peer) &&(tag==OMPI_ANY_TAG || elem->tag == tag)) {
                // found elem
                if (!current_oldest) {
                    current_oldest = elem;
                    oldest_bucket = my_bucket;
                }
                if (is_older(elem->seq_num,current_oldest->seq_num,current_seq_num)){
                    current_oldest = elem;
                    oldest_bucket = my_bucket;
                }
            }
            prev_elem = elem;
            elem = prev_elem->next;
        }
    }

            if (current_oldest) {
                // pthread does not allow to downgrade to readlock when having the writelock
                // this could be useful here, as we only need the bucket lock and rdlock for the following
                for (int j = 0; j < NUM_QUEEUS_IN_BUCKETS; ++j) {
                    if (!oldest_bucket->buckets[j].is_recv) {
                        // if bucket holds UMQ
                        bucket_node* prev_elem=NULL;
                        bucket_node* elem = oldest_bucket->buckets[j].bucket_head;
                        while (elem!=NULL) {
                            if (current_oldest==elem) {
                                // found elem
                                if (prev_elem==NULL) {
                                    oldest_bucket->buckets[j].bucket_head = elem->next;
                                }else {
                                    prev_elem->next=elem->next;
                                }
                                if (elem->next == NULL) {
                                    // removal of last element
                                    oldest_bucket->buckets[j].bucket_tail = NULL;
                                }

                                // unlock ALL
                                OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
                                for (int i =0;i<NUM_BUCKETS;++i) {
                                    OB1_MATCHING_UNLOCK(&map->buckets[i].mutex);
                                }
                                return to_memory_pool(map, elem);
                            }
                            prev_elem = elem;
                            elem = prev_elem->next;
                        }
                    }
                }// overflow bucket
                bucket_node* prev_elem=NULL;
                bucket_node* elem = oldest_bucket->overflow_bucket.bucket_head;
                while (elem!=NULL) {
                    if (elem==current_oldest) {
                        if (prev_elem==NULL) {
                            oldest_bucket->overflow_bucket.bucket_head = elem->next;
                        }else {
                            prev_elem->next=elem->next;
                        }
                        if (elem->next == NULL) {
                            // removal of last element
                            oldest_bucket->overflow_bucket.bucket_tail = prev_elem;
                        }

                        // unlock ALL
                        OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
                        for (int i =0;i<NUM_BUCKETS;++i) {
                            OB1_MATCHING_UNLOCK(&map->buckets[i].mutex);
                        }
                        return to_memory_pool(map, elem);
                    }
                    prev_elem = elem;
                    elem = prev_elem->next;
                }
                assert(0 && "Element lost" ); // unreachable
            }
    // else:
 // no match: append wildcard bucket

    bucket_node *new_elem = get_bucket_node(map);
    new_elem->tag = tag;
    new_elem->peer = peer;
    new_elem->next = NULL;
    new_elem->seq_num=__atomic_add_fetch(&map->seq_num,1,__ATOMIC_RELAXED);
    new_elem->is_recv = true;// must be a recv op as it has wildcard
    assert(__atomic_load_n(&new_elem->value,__ATOMIC_RELAXED)==NULL);
    *to_fill = &new_elem->value;
    if (map->wildcard_bucket.bucket_tail) {
        map->wildcard_bucket.bucket_tail->next = new_elem;

    }else {
        map->wildcard_bucket.bucket_head = new_elem;
    }
    map->wildcard_bucket.bucket_tail = new_elem;

    // unlock ALL
    OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
    for (int i =0;i<NUM_BUCKETS;++i) {
        OB1_MATCHING_UNLOCK(&map->buckets[i].mutex);
    }
    return NULL;

}

#else
static inline void *match_with_wildcard(hashmap *map, int tag, int peer, void*** to_fill)
{
    //lock ALL buckets
    OB1_MATCHING_LOCK(&map->wildcard_mutex);
    for (int i =0;i<NUM_BUCKETS;++i) {
        OB1_MATCHING_LOCK(&map->buckets[i].mutex);
    }

    bucket_node* current_oldest=NULL;
    bucket_collection* oldest_bucket=NULL;

    // traverse all buckets to find matching
    for (int i = 0; i < NUM_BUCKETS; ++i) {
        bucket_collection *my_bucket = &map->buckets[i];
        for (int j = 0; j < NUM_QUEEUS_IN_BUCKETS; ++j) {
            if (!my_bucket->buckets[j].is_recv){// if bucket holds UMQ
                bucket_node* prev_elem=NULL;
                bucket_node* elem = my_bucket->buckets[j].bucket_head;
                while (elem!=NULL) {
                    if ((peer==OMPI_ANY_SOURCE || elem->peer == peer) &&(tag==OMPI_ANY_TAG || elem->tag == tag)) {
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

                        //unlock ALL buckets
                        OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
                        for (int ii =0;ii<NUM_BUCKETS;++ii) {
                            OB1_MATCHING_UNLOCK(&map->buckets[ii].mutex);
                        }
                        return to_memory_pool(map, elem);
                    }
                    prev_elem = elem;
                    elem = prev_elem->next;
                }
            }
        }
        //overflow bucket
        bucket_node* prev_elem=NULL;
        bucket_node* elem = my_bucket->overflow_bucket.bucket_head;
        while (elem!=NULL) {
            if (!elem->is_recv && (peer==OMPI_ANY_SOURCE || elem->peer == peer) &&(tag==OMPI_ANY_TAG || elem->tag == tag)) {
                // found elem
                if (prev_elem==NULL) {
                    my_bucket->overflow_bucket.bucket_head= elem->next;
                }else {
                    prev_elem->next=elem->next;
                }
                if (elem->next == NULL) {
                    // removal of last element
                    my_bucket->overflow_bucket.bucket_tail = prev_elem;
                }
                //unlock ALL buckets
                OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
                for (int ii =0;ii<NUM_BUCKETS;++ii) {
                    OB1_MATCHING_UNLOCK(&map->buckets[ii].mutex);
                }
                return to_memory_pool(map, elem);
            }
            prev_elem= elem;
            elem = elem->next;
        }
    }

 // no match: append wildcard bucket

    bucket_node *new_elem = get_bucket_node(map);
    new_elem->tag = tag;
    new_elem->peer = peer;
    new_elem->next = NULL;
    new_elem->is_recv = true;// must be a recv op as it has wildcard
    assert(__atomic_load_n(&new_elem->value,__ATOMIC_RELAXED)==NULL);
    *to_fill = &new_elem->value;
    if (map->wildcard_bucket.bucket_tail) {
        map->wildcard_bucket.bucket_tail->next = new_elem;

    }else {
        map->wildcard_bucket.bucket_head = new_elem;
    }
    map->wildcard_bucket.bucket_tail = new_elem;

    //unlock ALL buckets
    OB1_MATCHING_UNLOCK(&map->wildcard_mutex);
    for (int ii =0;ii<NUM_BUCKETS;++ii) {
        OB1_MATCHING_UNLOCK(&map->buckets[ii].mutex);
    }
    return NULL;

}
#endif
#endif

// returns the match (and removed matched from queue)
// or inserts into the queue if no match and returns void
// basically combining the different matching queues
// to_fill will be set to void** where the actual payload data needs to be dropped, if elem is inserted
static inline void *get_match_or_insert(hashmap *map, int tag, int peer, void*** to_fill, bool is_recv)
{
    bucket_collection *my_bucket = &map->buckets[matching_hash_func(tag, peer)];

#ifdef WILDCARD_SUPPORT

    if ( OPAL_UNLIKELY(peer == OMPI_ANY_SOURCE || tag == OMPI_ANY_TAG))
    {
        assert(is_recv);// incoming msg cant have wildcards
        return match_with_wildcard(map, tag, peer, to_fill);
    }
    //pthread_rwlock_rdlock(&map->rwlock);
#endif

#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("%s try match (%d,%d)\n",is_recv?"recv posted":"msg arrived",tag,peer);
#endif
    //printf("access bucket %d (%d,%d,%d)\n",matching_hash_func(tag, peer),tag,peer,is_recv);
    //    bucket_collection *my_bucket = &map->buckets[matching_hash_func(tag, peer)];
    OB1_MATCHING_LOCK(&my_bucket->mutex);
#ifdef WILDCARD_SUPPORT
    if ( !is_recv && OPAL_UNLIKELY( map->wildcard_bucket.bucket_head!=NULL)) {
        bucket_node *elem_to_dequeue  = try_match_from_wildcard_prq(map,tag,peer,to_fill);
        if (elem_to_dequeue) {
            OB1_MATCHING_UNLOCK(&my_bucket->mutex);
            return to_memory_pool(map, elem_to_dequeue);
        }
#ifdef WILDCARD_NO_OVERTAKE_SUPPORT
        else {
            OB1_MATCHING_UNLOCK(&my_bucket->mutex);
            return NULL;
        }
#endif
    }
#endif

    // branchless find the correct bucket
    // if in overflow bucket: check for initialization and then goto back
    struct bucket* sub_bucket;
    find_sub_bucket:

    sub_bucket=&my_bucket->overflow_bucket;
    for (int i = 0; i < NUM_QUEEUS_IN_BUCKETS; ++i) {
#ifdef BRANCHLESS_BUCKET_SELECTOR
        // definitely branchless
        int condition = (my_bucket->bucket_infos[i].tag == tag) & (my_bucket->bucket_infos[i].peer == peer);
        uintptr_t mask = -condition;

        uintptr_t new_ptr = (uintptr_t)&my_bucket->buckets[i];
        uintptr_t old_ptr = (uintptr_t)sub_bucket;

        sub_bucket = (struct bucket*)((new_ptr & mask) | (old_ptr & ~mask));
#else
        // possibly branchless
        sub_bucket= my_bucket->bucket_infos[i].tag == tag & my_bucket->bucket_infos[i].peer == peer ? &my_bucket->buckets[i]: sub_bucket;
#endif

    }
    if (OPAL_LIKELY(sub_bucket!=&my_bucket->overflow_bucket)) {
        // if list empty or same mode: insert to queue
        if (sub_bucket->is_recv == is_recv
            || sub_bucket->bucket_head == NULL) {
            bucket_node *new_elem = get_bucket_node(map);
            new_elem->tag = tag;
            new_elem->peer = peer;
            new_elem->next = NULL;
#ifdef WILDCARD_NO_OVERTAKE_SUPPORT
            new_elem->seq_num=__atomic_add_fetch(&map->seq_num,1,__ATOMIC_RELAXED);
#endif
            new_elem->is_recv = is_recv;
            assert(__atomic_load_n(&new_elem->value,__ATOMIC_RELAXED)==NULL);
            *to_fill = &new_elem->value;
            insert_to_list(sub_bucket, new_elem, is_recv);
            OB1_MATCHING_UNLOCK(&my_bucket->mutex);
#if CUSTOM_MATCH_DEBUG_VERBOSE
            printf("add (%d,%d) to %s \n",tag,peer, is_recv?"prq":"umq");
#endif
            return NULL; // inserted into queue without a match
            } else {
                // not empty and holds the other queue
                // dequeue matching element
                bucket_node *elem_to_dequeue = remove_from_list(sub_bucket);
                OB1_MATCHING_UNLOCK(&my_bucket->mutex);
#if CUSTOM_MATCH_DEBUG_VERBOSE
                printf("matched (%d,%d) from %s \n",tag,peer, !is_recv?"prq":"umq");
#endif
                // free element
                return to_memory_pool(map, elem_to_dequeue);
            }
    }

    // check if all buckes where initialized
    //TODO at this point one can also update the direct buckets if necessary
    for (int i = 0; i < NUM_QUEEUS_IN_BUCKETS; ++i) {
        if (OPAL_UNLIKELY(my_bucket->bucket_infos[i].tag == -1)) {
            // initialize on first use
            my_bucket->bucket_infos[i].tag = tag;
            my_bucket->bucket_infos[i].peer = peer;
#if CUSTOM_MATCH_DEBUG_VERBOSE
            printf("initialize bucket %d_%d: (%d,%d)\n",matching_hash_func(tag, peer),i,tag,peer);
#endif
            goto find_sub_bucket;
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
    bucket_node *elem = my_bucket->overflow_bucket.bucket_head;

    while (elem != NULL) {
        if (elem->tag == tag && elem->peer == peer) {
            // found matching entry
            if (elem->is_recv == is_recv) {
                // same queue: insert at end
                bucket_node *new_elem = get_bucket_node(map);
                new_elem->tag = tag;
                new_elem->peer = peer;
                new_elem->next = NULL;
#ifdef WILDCARD_NO_OVERTAKE_SUPPORT
                new_elem->seq_num=__atomic_add_fetch(&map->seq_num,1,__ATOMIC_RELAXED);
#endif
                new_elem->is_recv = is_recv;
                assert(__atomic_load_n(&new_elem->value,__ATOMIC_RELAXED)==NULL);
                *to_fill = &new_elem->value;
                assert(my_bucket->overflow_bucket.bucket_tail);// since elem is in list, it contains at least one entry
                my_bucket->overflow_bucket.bucket_tail->next = new_elem;
                my_bucket->overflow_bucket.bucket_tail = new_elem;
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
                    my_bucket->overflow_bucket.bucket_head = elem->next;
                }
                // last elem
                if (my_bucket->overflow_bucket.bucket_tail == elem) {
                    my_bucket->overflow_bucket.bucket_tail = prev_elem;
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
#ifdef WILDCARD_NO_OVERTAKE_SUPPORT
    new_elem->seq_num=__atomic_add_fetch(&map->seq_num,1,__ATOMIC_RELAXED);
#endif
    new_elem->is_recv = is_recv;
    assert(__atomic_load_n(&new_elem->value,__ATOMIC_RELAXED)==NULL);
    *to_fill = &new_elem->value;

    if (my_bucket->overflow_bucket.bucket_tail == NULL) {
        assert(my_bucket->overflow_bucket.bucket_head == NULL);
        my_bucket->overflow_bucket.bucket_head = new_elem;
        my_bucket->overflow_bucket.bucket_tail = new_elem;
    } else {
        my_bucket->overflow_bucket.bucket_tail->next = new_elem;
        my_bucket->overflow_bucket.bucket_tail = new_elem;
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
#ifdef WILDCARD_SUPPORT
        OBJ_CONSTRUCT(&map->wildcard_mutex, opal_mutex_t);
#ifdef WILDCARD_NO_OVERTAKE_SUPPORT
    map->seq_num=0;
#endif
#endif

    OBJ_CONSTRUCT(&map->mutex, opal_mutex_t);
    for (int i = 0; i < NUM_BUCKETS; ++i) {
        OBJ_CONSTRUCT(&map->buckets[i].mutex, opal_mutex_t);
        for (int j = 0; j < NUM_QUEEUS_IN_BUCKETS; ++j) {
            map->buckets[i].bucket_infos[j].tag = -1;
            map->buckets[i].bucket_infos[j].peer = -1;
        }
    }
    return map;
}

static inline void match_map_destroy(hashmap *map)
{
#ifdef COUNT_COLLISIONS
    printf("Number of hash Collisions:%d\n", map->num_collisions);
#endif

#ifdef WILDCARD_SUPPORT
        OBJ_DESTRUCT(&map->wildcard_mutex);
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
        bucket_node *elem = map->buckets[i].overflow_bucket.bucket_head;
        while (elem != NULL) {
            bucket_node *next_elem = elem->next;
            free(elem);
            elem = next_elem;
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
