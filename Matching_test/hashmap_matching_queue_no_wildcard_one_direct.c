
#define OUTSIDE_CONFIGURATION
#define NO_DEBUGGING_UNDER_PERFORMANCE_TESTING

// settings for the hashmap implementation
#define NUM_QUEUES_IN_BUCKETS 1

#define IMPLEMENTATION_NAME_STRING "hashmap_no_wild_one_bin"
#define IMPLEMENTATION_NAME_FUNCTION_PREFIX hashmap_no_wild_one_bin

// include the actual implementation
#include "hashmap_matching_queue_impl.h"
