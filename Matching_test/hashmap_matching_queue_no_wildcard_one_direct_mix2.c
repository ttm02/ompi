
#define OUTSIDE_CONFIGURATION
#define NO_DEBUGGING_UNDER_PERFORMANCE_TESTING

// settings for the hashmap implementation
#define NUM_QUEUES_IN_BUCKETS 1


#define MATCHING_HASH_FUNC_IS_PROVIDED
#define NUM_BUCKETS 16
static inline int matching_hash_func(int tag, int peer)
{
    int maskA = 0b0101;
    int maskB = 0b1010;
    return (tag &maskB) & (peer & maskA);
}

#define IMPLEMENTATION_NAME_STRING "hashmap_no_wild_one_bin_mix2"
#define IMPLEMENTATION_NAME_FUNCTION_PREFIX hashmap_no_wild_one_bin_mix2

// include the actual implementation
#include "hashmap_matching_queue_impl.h"
