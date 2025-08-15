/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2007 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Voltaire All rights reserved.
 * Copyright (c) 2010      IBM Corporation.  All rights reserved.
 * Copyright (c) 2014-2018 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2021      Triad National Security, LLC. All rights reserved.
 * Copyright (c) 2021      Google, LLC. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_FIFO_H_HAS_BEEN_INCLUDED
#define OPAL_FIFO_H_HAS_BEEN_INCLUDED

#include "opal_config.h"
#include "opal/class/opal_lifo.h"

#include "opal/mca/threads/mutex.h"
#include "opal/sys/atomic.h"

BEGIN_C_DECLS

/* Atomic First In First Out lists. If we are in a multi-threaded environment then the
 * atomicity is insured via the compare-and-swap operation, if not we simply do a read
 * and/or a write.
 *
 * There is a trick. The ghost element at the end of the list. This ghost element has
 * the next pointer pointing to itself, therefore we cannot go past the end of the list.
 * With this approach we will never have a NULL element in the list, so we never have
 * to test for the NULL.
 */
struct opal_fifo_t {
    opal_object_t super;

    /** first element on the fifo */
    volatile opal_counted_pointer_t opal_fifo_head;
    /** last element on the fifo */
    volatile opal_counted_pointer_t opal_fifo_tail;

    /** list sentinel (always points to self) */
    opal_list_item_t opal_fifo_ghost;
};

typedef struct opal_fifo_t opal_fifo_t;

OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_fifo_t);

static inline opal_list_item_t *opal_fifo_head(opal_fifo_t *fifo)
{
    return (opal_list_item_t *) fifo->opal_fifo_head.data.item;
}

static inline opal_list_item_t *opal_fifo_tail(opal_fifo_t *fifo)
{
    return (opal_list_item_t *) fifo->opal_fifo_tail.data.item;
}

/* The ghost pointer will never change. The head will change via an atomic
 * compare-and-swap. On most architectures the reading of a pointer is an
 * atomic operation so we don't have to protect it.
 */
static inline bool opal_fifo_is_empty(opal_fifo_t *fifo)
{
    return opal_fifo_head(fifo) == &fifo->opal_fifo_ghost;
}

#if OPAL_HAVE_ATOMIC_COMPARE_EXCHANGE_128 && !OPAL_HAVE_ATOMIC_LLSC_PTR

/* Add one element to the FIFO. We will return the last head of the list
 * to allow the upper level to detect if this element is the first one in the
 * list (if the list was empty before this operation).
 */
static inline opal_list_item_t *opal_fifo_push_atomic(opal_fifo_t *fifo, opal_list_item_t *item)
{
    opal_counted_pointer_t tail, new_tail;
    const opal_list_item_t *const ghost = &fifo->opal_fifo_ghost;
    opal_list_item_t *prev;

    /* Initialize the new item */
    __atomic_store_n(&item->opal_list_next, ghost, __ATOMIC_RELAXED);

    /* Atomically update the tail pointer */
    do {
        opal_read_counted_pointer(&fifo->opal_fifo_tail, &tail);
        new_tail.data.counter = tail.data.counter + 1;
        new_tail.data.item = (intptr_t) item;
    } while (!opal_atomic_compare_exchange_strong_rel_128(&fifo->opal_fifo_tail.atomic_value,
                                                          &tail.value, new_tail.value));

    prev = (opal_list_item_t *) tail.data.item;

    /* Link the previous tail to the new item */
    if (prev == ghost) {
        /* FIFO was empty - need to update head as well */
        opal_counted_pointer_t head, new_head;
        opal_read_counted_pointer(&fifo->opal_fifo_head, &head);
        new_head.data.counter = head.data.counter + 1;
        new_head.data.item = (intptr_t) item;
        // we are the only one, that can modify head at that point:
        // other removes will see empty list
        // other insert will insert after us and only update tail
        __atomic_store_n(&fifo->opal_fifo_head.atomic_value, new_head.value, __ATOMIC_RELEASE);
    } else {
        /* Link previous item to new item */
        __atomic_store_n(&prev->opal_list_next, item, __ATOMIC_RELEASE);
    }

    return prev;
}

/* Retrieve one element from the FIFO. If we reach the ghost element then the FIFO
 * is empty so we return NULL.
 */
static inline opal_list_item_t *opal_fifo_pop_atomic(opal_fifo_t *fifo)
{
    opal_counted_pointer_t head, new_head;
    opal_list_item_t *item, *next;
    opal_list_item_t *ghost = &fifo->opal_fifo_ghost;
    // int attempt = 0;

    while (1) {
        /* Prevent excessive spinning */
        /*if (++attempt == 10) {
            opal_lifo_release_cpu();
            attempt = 0;
        }*/

        /* Read current head */
        opal_read_counted_pointer(&fifo->opal_fifo_head, &head);
        item = (opal_list_item_t *) head.data.item;

        if (item == ghost) { // is empty
            return NULL;
        }

        next = (opal_list_item_t *) __atomic_load_n(&item->opal_list_next, __ATOMIC_ACQUIRE);

        new_head.data.counter = head.data.counter + 1;
        new_head.data.item = (intptr_t) next;

        if (opal_atomic_compare_exchange_strong_rel_128(&fifo->opal_fifo_head.atomic_value,
                                                        &head.value, new_head.value)) {
            break; /* Successfully updated head */
        }
    }

    /* Handle the case where we might have emptied the FIFO */
    if (next == ghost) {
        opal_counted_pointer_t tail;
        /* We potentially emptied the FIFO - try to update tail */
        opal_read_counted_pointer(&fifo->opal_fifo_tail, &tail);

        if ((opal_list_item_t *) tail.data.item == item) {
            /* Tail is still pointing to the item we just removed */
            opal_counted_pointer_t new_tail;
            new_tail.data.counter = tail.data.counter + 1;
            new_tail.data.item = (intptr_t) ghost;

            /* Try update tail to ghost - if this fails, another push happened */
            if (opal_atomic_compare_exchange_strong_rel_128(&fifo->opal_fifo_tail.atomic_value,
                                                             &tail.value, new_tail.value)) {
                //update successful, done
            }else {
                // tail was updated
                goto tail_updated;
            }
            // else
        } else {
            opal_list_item_t *new_next;
        // else the tail was updated
        tail_updated:
            // on tail update, other T has pushed a new item
            // since tail still linked to us, we expect the other T to update next
            new_next = ghost;
            while (new_next == ghost) {
                new_next = (opal_list_item_t*)__atomic_load_n(&item->opal_list_next, __ATOMIC_ACQUIRE);
            }
            // at this point: we are the only one that can update head
            // other pop will see empty list
            // other push will add behind us
            new_head.data.counter++;
            new_head.data.item = (intptr_t) new_next;
            __atomic_store_n(&fifo->opal_fifo_head.atomic_value, new_head.value, __ATOMIC_RELEASE);
        }
    }
    /* Clean up the popped item */
    __atomic_store_n(&item->opal_list_next, NULL, __ATOMIC_RELAXED);

    return item;
}

#else

/* When compare-and-set 128 is not available we avoid the ABA problem by
 * using a spin-lock on the head (using the head counter). Otherwise
 * the algorithm is identical to the compare-and-set 128 version. */
static inline opal_list_item_t *opal_fifo_push_atomic(opal_fifo_t *fifo, opal_list_item_t *item)
{
    const opal_list_item_t *const ghost = &fifo->opal_fifo_ghost;
    opal_list_item_t *tail_item;

    item->opal_list_next = (opal_list_item_t *) ghost;

    opal_atomic_wmb();

    /* try to get the tail */
    tail_item = (opal_list_item_t *) opal_atomic_swap_ptr(&fifo->opal_fifo_tail.data.item,
                                                          (intptr_t) item);

    opal_atomic_wmb();

    if (ghost == tail_item) {
        /* update the head */
        fifo->opal_fifo_head.data.item = (intptr_t) item;
    } else {
        /* update previous item */
        tail_item->opal_list_next = item;
    }

    opal_atomic_wmb();

    return (opal_list_item_t *) tail_item;
}

/* Retrieve one element from the FIFO. If we reach the ghost element then the FIFO
 * is empty so we return NULL.
 */
static inline opal_list_item_t *opal_fifo_pop_atomic(opal_fifo_t *fifo)
{
    const opal_list_item_t *const ghost = &fifo->opal_fifo_ghost;

#    if OPAL_HAVE_ATOMIC_LLSC_PTR
    register opal_list_item_t *item, *next;
    int attempt = 0, ret = 0;

    /* use load-linked store-conditional to avoid ABA issues */
    do {
        if (++attempt == 5) {
            /* deliberately suspend this thread to allow other threads to run. this should
             * only occur during periods of contention on the lifo. */
            opal_lifo_release_cpu();
            attempt = 0;
        }

        opal_atomic_ll_ptr(&fifo->opal_fifo_head.data.item, item);
        if (ghost == item) {
            if ((intptr_t) ghost == fifo->opal_fifo_tail.data.item) {
                return NULL;
            }

            /* fifo does not appear empty. wait for the fifo to be made
             * consistent by conflicting thread. */
            continue;
        }

        next = (opal_list_item_t *) item->opal_list_next;
        opal_atomic_sc_ptr(&fifo->opal_fifo_head.data.item, next, ret);
    } while (!ret);

#    else
    opal_list_item_t *item, *next;

    /* protect against ABA issues by "locking" the head */
    do {
        if (!opal_atomic_swap_32((opal_atomic_int32_t *) &fifo->opal_fifo_head.data.counter, 1)) {
            break;
        }

        opal_atomic_wmb();
    } while (1);

    opal_atomic_wmb();

    item = opal_fifo_head(fifo);
    if (ghost == item) {
        fifo->opal_fifo_head.data.counter = 0;
        return NULL;
    }

    next = (opal_list_item_t *) item->opal_list_next;
    fifo->opal_fifo_head.data.item = (uintptr_t) next;
#    endif

    if (ghost == next) {
        void *tmp = item;

        if (!opal_atomic_compare_exchange_strong_ptr(&fifo->opal_fifo_tail.data.item,
                                                     (intptr_t *) &tmp, (intptr_t) ghost)) {
            do {
                opal_atomic_rmb();
            } while (ghost == item->opal_list_next);

            fifo->opal_fifo_head.data.item = (intptr_t) item->opal_list_next;
        }
    }

    opal_atomic_wmb();

    /* unlock the head */
    fifo->opal_fifo_head.data.counter = 0;

    item->opal_list_next = NULL;

    return item;
}

#endif

/* single threaded versions of push/pop */
static inline opal_list_item_t *opal_fifo_push_st(opal_fifo_t *fifo, opal_list_item_t *item)
{
    opal_list_item_t *prev = opal_fifo_tail(fifo);

    item->opal_list_next = &fifo->opal_fifo_ghost;

    fifo->opal_fifo_tail.data.item = (intptr_t) item;
    if (&fifo->opal_fifo_ghost == opal_fifo_head(fifo)) {
        fifo->opal_fifo_head.data.item = (intptr_t) item;
    } else {
        prev->opal_list_next = item;
    }

    return (opal_list_item_t *) item->opal_list_next;
}

static inline opal_list_item_t *opal_fifo_pop_st(opal_fifo_t *fifo)
{
    opal_list_item_t *item = opal_fifo_head(fifo);

    if (item == &fifo->opal_fifo_ghost) {
        return NULL;
    }

    fifo->opal_fifo_head.data.item = (intptr_t) item->opal_list_next;
    if (&fifo->opal_fifo_ghost == opal_fifo_head(fifo)) {
        fifo->opal_fifo_tail.data.item = (intptr_t) &fifo->opal_fifo_ghost;
    }

    item->opal_list_next = NULL;
    return item;
}

/* push/pop versions conditioned off opal_using_threads() */
static inline opal_list_item_t *opal_fifo_push(opal_fifo_t *fifo, opal_list_item_t *item)
{
    if (opal_using_threads()) {
        return opal_fifo_push_atomic(fifo, item);
    }

    return opal_fifo_push_st(fifo, item);
}

static inline opal_list_item_t *opal_fifo_pop(opal_fifo_t *fifo)
{
    if (opal_using_threads()) {
        return opal_fifo_pop_atomic(fifo);
    }

    return opal_fifo_pop_st(fifo);
}

END_C_DECLS

#endif /* OPAL_FIFO_H_HAS_BEEN_INCLUDED */
