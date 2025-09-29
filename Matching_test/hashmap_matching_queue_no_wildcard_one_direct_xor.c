
#define OUTSIDE_CONFIGURATION
#define NO_DEBUGGING_UNDER_PERFORMANCE_TESTING

// settings for the hashmap implementation
#define NUM_QUEUES_IN_BUCKETS 1


#define MATCHING_HASH_FUNC_IS_PROVIDED
#define NUM_BUCKETS 16
static inline int matching_hash_func(int tag, int peer)
{
    int mask = 0x7FFFFFFF; // only sign bit not set
    // tag may be negative on some internal communication
    return ((tag& mask) ^ peer) % NUM_BUCKETS;
}

#define IMPLEMENTATION_NAME_STRING "hashmap_no_wild_one_bin_xor"
#define IMPLEMENTATION_NAME_FUNCTION_PREFIX hashmap_no_wild_one_bin_xor




// include the actual implementation
#include "hashmap_matching_queue_impl.h"
