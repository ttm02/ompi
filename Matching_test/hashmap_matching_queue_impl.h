#ifndef HASHMAP_MATCHING_QUEUE_IMPL
#define HASHMAP_MATCHING_QUEUE_IMPL

#include "matching_performance.h"

#include <stdbool.h>

#include <stdlib.h>



#ifndef OUTSIDE_CONFIGURATION
_Static_assert(0 && "Not correctly included")
#endif

#ifndef IMPLEMENTATION_NAME_STRING
_Static_assert(0 && "Not correctly included")
#endif

#define MUST_BE_STRING_LITERAL(x) \
_Static_assert( \
__builtin_types_compatible_p(__typeof__(x), const char [sizeof(x)]) || \
__builtin_types_compatible_p(__typeof__(x), char [sizeof(x)]), \
"Must be a string literal" \
)

MUST_BE_STRING_LITERAL(IMPLEMENTATION_NAME_STRING);

#ifndef IMPLEMENTATION_NAME_FUNCTION_PREFIX
_Static_assert(0 && "Not correctly included")
#endif

// preprocessor-magic to make the function names
#define CONCAT(prefix, name) prefix##name
#define MAKE_NAME(prefix, name) CONCAT(prefix, name)

#include "../ompi/mca/pml/ob1/custommatch/pml_ob1_custom_match_hashmap.h"

void * MAKE_NAME(IMPLEMENTATION_NAME_FUNCTION_PREFIX, _init_matching_queues)()
{
    return (void*)match_map_init();
}

void MAKE_NAME(IMPLEMENTATION_NAME_FUNCTION_PREFIX, _destroy_matching_queues)(void *matching_queues)
{
    match_map_destroy((hashmap*)matching_queues);
}

bool MAKE_NAME(IMPLEMENTATION_NAME_FUNCTION_PREFIX, _try_match_incoming)(void *matching_queue, int tag, int src, void *payload)
{
    bool retval = true;
    void **to_fill = NULL;
    void *recv_req = get_match_or_insert((hashmap*)matching_queue, tag, src, &to_fill, false);
    if (recv_req) {
        retval = true;
    } else {
        // no match => enqueue to unexpected queue
        __atomic_store_n(to_fill, payload, __ATOMIC_RELAXED);
        retval = false;
    }

    return retval;
}

bool MAKE_NAME(IMPLEMENTATION_NAME_FUNCTION_PREFIX, _try_match_receive)(void *matching_queue, int tag, int src, void *payload)
{
    bool retval = true;
    void **to_fill = NULL;

    void *msg_found = get_match_or_insert((hashmap*)matching_queue, tag, src, &to_fill, true);

    if (msg_found) {

        retval = true;
    } else {
        __atomic_store_n(to_fill, payload, __ATOMIC_RELAXED);
        retval = false;
    }

    return retval;
}

__attribute__((constructor))
void MAKE_NAME(IMPLEMENTATION_NAME_FUNCTION_PREFIX, _register_implementation)()
{
    implementation_info info;
    info.name=IMPLEMENTATION_NAME_STRING;
    info.init_matching_queues = &MAKE_NAME(IMPLEMENTATION_NAME_FUNCTION_PREFIX, _init_matching_queues);
    info.destroy_matching_queues = &MAKE_NAME(IMPLEMENTATION_NAME_FUNCTION_PREFIX, _destroy_matching_queues);
    info.try_match_incoming = &MAKE_NAME(IMPLEMENTATION_NAME_FUNCTION_PREFIX, _try_match_incoming);
    info.try_match_receive = &MAKE_NAME(IMPLEMENTATION_NAME_FUNCTION_PREFIX, _try_match_receive);
    info.next_implementation=NULL;
    register_implementation(&info);
}

#endif // HASHMAP_MATCHING_QUEUE_IMPL
