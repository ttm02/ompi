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
    bool is_umq;
    void *value;
} bucket_node;

struct bucket {
    int tag;
    int peer;
    bucket_node *bucket_head;
    bucket_node *bucket_tail;
    bool is_umq;// can only be changed while locked
};

typedef struct bucket_collection {
    // TODO re-order memory to use array of structs?
    //  efficient access when no collisions are present
    int tag[NUM_QUEEUS_IN_BUCKETS];
    int peer[NUM_QUEEUS_IN_BUCKETS];
    bucket_node *bucket_head[NUM_QUEEUS_IN_BUCKETS];
    bucket_node *bucket_tail[NUM_QUEEUS_IN_BUCKETS];

    // other bucket used on more collisions: need traversal and lock
    opal_mutex_t mutex; // if locking is necessary
    bucket_node *other_keys_bucket_head;
    bucket_node *other_keys_bucket_tail;
} bucket_collection;

typedef struct hashmap {
    bucket_collection buckets[NUM_BUCKETS];
    bucket_node *memory_pool;
#ifdef COUNT_COLLISIONS
    int num_collisions;
#endif
} hashmap;

// same name as used in other implementations
typedef hashmap custom_match_prq;

// simple hash function should suffice
// TODO evaluate other hash functions?
inline int hash_func(int tag, int peer)
{
    return tag + peer % NUM_BUCKETS;
}

static inline int custom_match_prq_cancel(custom_match_prq *list, void *req)
{
    assert(0 && "Not implemented");
    // TODO implement
    // this is the most inefficient operation, as we need to go through all buckets
    // luckily we dont need to lock for that, as if another T matches in the mean time cancel just
    // does nothing
    return 0;
}

static inline void to_memory_pool(hashmap *map, bucket_node *node)
{
    // TODO implement
}

static inline void get_bucket_node(hashmap *map)
{
    // fetch from memory pool or allocate if pool is empty
    // TODO implement
}

//TODO there is still a threading problem with this design
// while inserting into teh list, another T can swap the list status

// returns the match (and removed matched from queue)
// or inserts into the queue if no match and returns void
// basically combining teh different matching queues
static inline void *get_match_or_insert(hashmap *map, int tag, int peer, void *payload, bool is_umq)
{
    bucket my_bucket = map->buckets[hash_func(tag, peer)];

    for (int i = 0; i < NUM_QUEEUS_IN_BUCKETS; ++i) {
        if (OPAL_UNLIKELY(my_bucket.tag[i] == -1)) {
            // initialize on first use
            OB1_MATCHING_LOCK(&my_bucket.mutex);
            if (OPAL_UNLIKELY(my_bucket.tag[i] == -1)) {
                my_bucket.tag[i] = tag;
                my_bucket.peer[i] = peer;
            }
            OB1_MATCHING_UNLOCK(&my_bucket.mutex);
        }
        if (OPAL_LIKELY(my_bucket.tag[i] == tag && my_bucket.peer[i] == peer)) {
            // found correct bucket

            // if list empty or same mode: insert to queue
            if (my_bucket.bucket_head[i] == NULL || my_bucket.bucket_head[i]->is_umq == is_umq) {
                bucket_node *new_elem = get_bucket_node(map);
                new_elem->tag = tag;
                new_elem->peer = peer;
                new_elem->next = NULL;
                new_elem->is_umq = is_umq;
                new_elem->value = payload;

                if (my_bucket.bucket_head[i] == NULL || my_bucket.bucket_tail[i] == NULL) {
                    // on empty list
                    // if head==NULL and tail != null: operation fail retry ofter other T finished their operation
                    bucket_node *prev_tail = NULL; // otherwise tail was updated

                    if (__atomic_compare_exchange_n(&my_bucket.bucket_tail[i], &prev_tail, new_elem,
                                                    false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
                        // successfully inserted
                        // no need for atomic CAS: other T would synchronize through previous CAS,
                        // and not update head ptr on fail
                        my_bucket.bucket_head[i] = new_elem;
                    } else {
                        // fail: retry
                        return get_match_or_insert(map, tag, peer, payload, is_umq);
                    }
                } else {
                    bucket_node *prev_tail_successor = NULL; // otherwise tail was updated
                    if (__atomic_compare_exchange_n(&my_bucket.bucket_tail[i]->next,
                                                    &prev_tail_successor, new_elem, false,
                                                    __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
                        // successfully inserted
                        // no need for atomic CAS: other T would synchronize through previous CAS,
                        // and not update tail ptr on fail
                        my_bucket.bucket_tail[i] = new_elem;
                        return NULL;
                    } else {
                        // fail: retry
                        return get_match_or_insert(map, tag, peer, payload, is_umq);
                    }
                }

                return NULL;
            } else {
                // dequeue
                bucket_node *elem_to_dequeue = my_bucket.bucket_head[i];
                // try to remove head node
                if (__atomic_compare_exchange_n(&my_bucket.bucket_head[i], &elem_to_dequeue,
                                                elem_to_dequeue->next, false, __ATOMIC_ACQ_REL,
                                                __ATOMIC_ACQUIRE)) {
                    // success, removed node
                    if (my_bucket.bucket_tail[i] == elem_to_dequeue) {
                        // this is last element in list
                        bucket_node *null_node = NULL;
                        // mark this element as invalid // prevents other T from updating the tail
                        // ptr until we updated it
                        if (__atomic_compare_exchange_n(&elem_to_dequeue->next, &null_node,
                                                        (bucket_node *) 1, false, __ATOMIC_ACQ_REL,
                                                        __ATOMIC_ACQUIRE)) {
                            my_bucket.bucket_tail[i]
                                = NULL; // previous CAS will synchronize this access
                            elem_to_dequeue->next = NULL;
                        } else {
                            // other T has inserted into the list in the meantime: fix head ptr
                            // no other T can access headptr at the time, as a null headpointer will
                            // only be updated on null tail pointer null tailpointer is not
                            // possible, since other T inserted something into the list
                            __atomic_store_n(&my_bucket.bucket_head[i], elem_to_dequeue->next,
                                             __ATOMIC_RELEASE);
                        }
                    } // end handling dequeue of last element
                    void *retval = elem_to_dequeue->value;
                    to_memory_pool(map, elem_to_dequeue); // invalidated elem
                    return retval;

                } else {
                    // other T has changed something: restart operation
                    return get_match_or_insert(map, tag, peer, payload, is_umq);
                }
            }

            // if list has same mode: match
            return my_bucket.bucket_head[i]->value;
        }
    }
    // multiple hash collisions
#ifdef COUNT_COLLISIONS
    __atomic_add_fetch(&map->num_collisions, 1, __ATOMIC_ACQ_REL);
#endif

    return 0;
}

static inline void *get_match_or_insert_to_umq(custom_match_prq *map, int tag, int peer,
                                               void *payload)
{
    assert(peer != OMPI_ANY_SOURCE);
    assert(tag != OMPI_ANY_TAG);
    return get_match_or_insert(map, tag, peer, payload, true);
}

static inline void *get_match_or_insert_to_prq(custom_match_prq *map, int tag, int peer,
                                               void *payload)
{

    assert(peer != OMPI_ANY_SOURCE);
    assert(tag != OMPI_ANY_TAG);
    return get_match_or_insert(map, tag, peer, payload, false);
}

static inline void *custom_match_prq_find_dequeue_verify(custom_match_prq *list, int tag, int peer)
{
    OB1_MATCHING_LOCK(&list->mutex);
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("custom_match_prq_find_dequeue_verify list: %x:%d tag: %x peer: %x\n", list, list->size,
           tag, peer);
#endif
    custom_match_prq_node *prev = 0;
    custom_match_prq_node *elem = list->head;
    int result;

    while (elem) {
        result = ((elem->tag & elem->tmask) == (tag & elem->tmask))
                 && ((elem->src & elem->smask) == (peer & elem->smask));
        if (result) {
            void *payload = elem->value;
            elem->tag = ~0;
            elem->tmask = ~0;
            elem->src = ~0;
            elem->smask = ~0;
            elem->value = 0;
            if (prev) {
                prev->next = elem->next;
            } else {
                list->head = elem->next;
            }
            if (!elem->next) {
                list->tail = prev;
            }
            elem->next = list->pool;
            list->pool = elem;
#if CUSTOM_MATCH_DEBUG_VERBOSE
            printf("%x == %x added to the pool\n", elem, list->pool);
#endif
            list->size--;
            mca_pml_base_request_t *req = (mca_pml_base_request_t *) payload;
#if CUSTOM_MATCH_DEBUG_VERBOSE
            printf("Found list: %x tag: %x peer: %x\n", list, req->req_tag, req->req_peer);
#endif
            OB1_MATCHING_UNLOCK(&list->mutex);
            return payload;
        }
        prev = elem;
        elem = elem->next;
    }
    OB1_MATCHING_UNLOCK(&list->mutex);
    return 0;
}

static inline void custom_match_prq_append(custom_match_prq *list, void *payload, int tag,
                                           int source)
{

    int32_t mask_tag, mask_src;
    if (source == OMPI_ANY_SOURCE) {
        mask_src = 0;
    } else {
        mask_src = ~0;
    }
    if (tag == OMPI_ANY_TAG) {
        mask_tag = 0;
    } else {
        mask_tag = ~0;
    }
    mca_pml_base_request_t *req = (mca_pml_base_request_t *) payload;
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("custom_match_prq_append list: %x tag: %x source: %x tag: %x peer: %x\n", list, tag,
           source, req->req_tag, req->req_peer);
#endif
    OB1_MATCHING_LOCK(&list->mutex);
    int i;
    custom_match_prq_node *elem;
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("%x next elem in the pool\n", list->pool);
#endif
    if (list->pool) {
        elem = list->pool;
        list->pool = list->pool->next;
    } else {
        elem = malloc(sizeof(custom_match_prq_node));
    }
    elem->next = 0;
    if (list->tail) {
        list->tail->next = elem;
        list->tail = elem;
    } else {
        list->head = elem;
        list->tail = elem;
    }

    elem = list->tail;
    elem->tag = tag;
    elem->tmask = mask_tag;
    elem->src = source;
    elem->smask = mask_src;
    elem->value = payload;
    list->size++;
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("Exiting custom_match_prq_append\n");
#endif
    OB1_MATCHING_UNLOCK(&list->mutex);
}

static inline int custom_match_prq_size(custom_match_prq *list)
{
    return list->size;
}

static inline custom_match_prq *custom_match_prq_init()
{
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("custom_match_prq_init\n");
#endif
    custom_match_prq *list = malloc(sizeof(custom_match_prq));
    OBJ_CONSTRUCT(&list->mutex, opal_mutex_t);
    list->head = 0;
    list->tail = 0;
    list->pool = 0;
    list->size = 0;
    return list;
}

static inline void custom_match_prq_destroy(custom_match_prq *list)
{
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("custom_match_prq_destroy\n");
#endif
    custom_match_prq_node *elem;
    int i = 0;
    int j = 0;
    while (list->head) {
        elem = list->head;
        list->head = list->head->next;
        free(elem);
        i++;
    }
    while (list->pool) {
        elem = list->pool;
        list->pool = list->pool->next;
        free(elem);
        j++;
    }
    OBJ_DESTRUCT(&list->mutex);
    free(list);
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("Number of prq elements destroyed = %d %d\n", i, j);
#endif
}

static inline void custom_match_print(custom_match_prq *list)
{
    custom_match_prq_node *elem;
    int i = 0;
    int j = 0;
    printf("Elements in the list (this is currently only partially implemented):\n");
    for (elem = list->head; elem; elem = elem->next) {
        printf("This is the %d linked list element\n", ++i);
        printf("%d The key is %d, the mask is %d, the value is %ld\n", i, elem->tag, elem->tmask,
               elem->value);
        i++;
    }
}

static inline void custom_match_prq_dump(custom_match_prq *list)
{
    opal_list_item_t *item;
    char cpeer[64], ctag[64];

    custom_match_prq_node *elem;
    int i = 0;
    int j = 0;
    printf("Elements in the list:\n");
    for (elem = list->head; elem; elem = elem->next) {
        printf("This is the %d linked list element\n", ++i);
        if (elem->value) {
            mca_pml_base_request_t *req = (mca_pml_base_request_t *) elem->value;
            if (OMPI_ANY_SOURCE == req->req_peer)
                snprintf(cpeer, 64, "%s", "ANY_SOURCE");
            else
                snprintf(cpeer, 64, "%d", req->req_peer);
            if (OMPI_ANY_TAG == req->req_tag)
                snprintf(ctag, 64, "%s", "ANY_TAG");
            else
                snprintf(ctag, 64, "%d", req->req_tag);
            opal_output(
                0,
                "req %p peer %s tag %s addr %p count %lu datatype %s [%p] [%s %s] req_seq %" PRIu64,
                (void *) req, cpeer, ctag, (void *) req->req_addr, req->req_count,
                (0 != req->req_count ? req->req_datatype->name : "N/A"), (void *) req->req_datatype,
                (req->req_pml_complete ? "pml_complete" : ""),
                (req->req_free_called ? "freed" : ""), req->req_sequence);
        }
    }
}

// UMQ below.

typedef struct custom_match_umq_node {
    int tag;
    int src;
    struct custom_match_umq_node *next;
    void *value;
} custom_match_umq_node;

typedef struct custom_match_umq {
    opal_mutex_t mutex;
    custom_match_umq_node *head;
    custom_match_umq_node *tail;
    custom_match_umq_node *pool;
    int size;
} custom_match_umq;

static inline void custom_match_umq_dump(custom_match_umq *list);

static inline void *custom_match_umq_find_verify_hold(custom_match_umq *list, int tag, int peer,
                                                      custom_match_umq_node **hold_prev,
                                                      custom_match_umq_node **hold_elem,
                                                      int *hold_index)
{
    OB1_MATCHING_LOCK(&list->mutex);
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("custom_match_umq_find_verify_hold list: %x:%d tag: %x peer: %x\n", list, list->size,
           tag, peer);
    custom_match_umq_dump(list);
#endif
    custom_match_umq_node *prev = 0;
    custom_match_umq_node *elem = list->head;
    int result;

    int tmask = ~0;
    int smask = ~0;
    if (peer == OMPI_ANY_SOURCE) {
        smask = 0;
    }

    if (tag == OMPI_ANY_TAG) {
        tmask = 0;
    }

    tag = tag & tmask;
    peer = peer & smask;

    while (elem) {
        result = ((elem->tag & tmask) == tag) && ((elem->src & smask) == peer);
        if (result) {
#if CUSTOM_MATCH_DEBUG_VERBOSE
            printf("Found list: %x tag: %x peer: %x\n", list, tag, peer);
#endif
            *hold_prev = prev;
            *hold_elem = elem;
            *hold_index = 0;
            OB1_MATCHING_UNLOCK(&list->mutex);
            return elem->value;
        }
        prev = elem;
        elem = elem->next;
    }
    OB1_MATCHING_UNLOCK(&list->mutex);
    return 0;
}

static inline void custom_match_umq_remove_hold(custom_match_umq *list, custom_match_umq_node *prev,
                                                custom_match_umq_node *elem, int i)
{
    OB1_MATCHING_LOCK(&list->mutex);
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("custom_match_umq_find_remove_hold %x %x %x\n", prev, elem, i);
#endif
    elem->tag = ~0;
    elem->src = ~0;
    elem->value = 0;
    if (prev) {
        prev->next = elem->next;
    } else {
        list->head = elem->next;
    }
    if (!elem->next) {
        list->tail = prev;
    }
    elem->next = list->pool;
    list->pool = elem;
    list->size--;
    OB1_MATCHING_UNLOCK(&list->mutex);
}

static inline void custom_match_umq_append(custom_match_umq *list, int tag, int source,
                                           void *payload)
{
    OB1_MATCHING_LOCK(&list->mutex);
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("custom_match_umq_append list: %x payload: %x tag: %d src: %d\n", list, payload, tag,
           source);
#endif
    int i;
    custom_match_umq_node *elem;
    list->size++;
    if (list->pool) {
#if CUSTOM_MATCH_DEBUG_VERBOSE
        printf("Grab an element from the pool\n");
#endif
        elem = list->pool;
        list->pool = list->pool->next;
    } else {
#if CUSTOM_MATCH_DEBUG_VERBOSE
        printf("Make a new element\n");
#endif
        elem = malloc(sizeof(custom_match_umq_node));
    }
    elem->next = 0;
    if (list->tail) {
#if CUSTOM_MATCH_DEBUG_VERBOSE
        printf("Append to list of elems\n");
#endif
        list->tail->next = elem;
        list->tail = elem;
    } else {
#if CUSTOM_MATCH_DEBUG_VERBOSE
        printf("New Elem is only Elem\n");
#endif
        list->head = elem;
        list->tail = elem;
    }

    elem = list->tail;
    elem->tag = tag;
    elem->src = source;
    elem->value = payload;
#if CUSTOM_MATCH_DEBUG_VERBOSE
    custom_match_umq_dump(list);
#endif
    OB1_MATCHING_UNLOCK(&list->mutex);
}

static inline custom_match_umq *custom_match_umq_init()
{
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("custom_match_umq_init\n");
#endif
    custom_match_umq *list = malloc(sizeof(custom_match_umq));
    OBJ_CONSTRUCT(&list->mutex, opal_mutex_t);
    list->head = 0;
    list->tail = 0;
    list->pool = 0;
    list->size = 0;
    return list;
}

static inline void custom_match_umq_destroy(custom_match_umq *list)
{
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("custom_match_umq_destroy\n");
#endif
    custom_match_umq_node *elem;
    int i = 0;
    int j = 0;
    while (list->head) {
        elem = list->head;
        list->head = list->head->next;
        free(elem);
        i++;
    }
    while (list->pool) {
        elem = list->pool;
        list->pool = list->pool->next;
        free(elem);
        j++;
    }
    OBJ_DESTRUCT(&list->mutex);
    free(list);
#if CUSTOM_MATCH_DEBUG_VERBOSE
    printf("Number of umq elements destroyed = %d %d\n", i, j);
#endif
}

static inline int custom_match_umq_size(custom_match_umq *list)
{
    return list->size;
}

static inline void custom_match_umq_dump(custom_match_umq *list)
{
    char cpeer[64], ctag[64];

    custom_match_umq_node *elem;
    int i = 0;
    int j = 0;
    printf("Elements in the list:\n");
    for (elem = list->head; elem; elem = elem->next) {
        printf("This is the %d linked list element\n", ++i);
        if (elem->value) {
            mca_pml_ob1_recv_frag_t *req = (mca_pml_ob1_recv_frag_t *) elem->value;
            printf("%x %x %x\n", elem->value, req->hdr.hdr_match.hdr_tag,
                   req->hdr.hdr_match.hdr_src);
            if (OMPI_ANY_SOURCE == req->hdr.hdr_match.hdr_src)
                snprintf(cpeer, 64, "%s", "ANY_SOURCE");
            else
                snprintf(cpeer, 64, "%d", req->hdr.hdr_match.hdr_src);
            if (OMPI_ANY_TAG == req->hdr.hdr_match.hdr_tag)
                snprintf(ctag, 64, "%s", "ANY_TAG");
            else
                snprintf(ctag, 64, "%d", req->hdr.hdr_match.hdr_tag);
            // opal_output(0, "peer %s tag %s",// addr %p count %lu datatype %s [%p] [%s %s] req_seq
            // %" PRIu64,
            //         /*(void*) req,*/ cpeer, ctag,
            //(void*) req->req_addr, req->req_count,
            //(0 != req->req_count ? req->req_datatype->name : "N/A"),
            //(void*) req->req_datatype,
            //(req->req_pml_complete ? "pml_complete" : ""),
            //(req->req_free_called ? "freed" : ""),
            // req->req_sequence);
            //           );
        }
    }
}

#endif
