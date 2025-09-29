
#define OUTSIDE_CONFIGURATION
#define NO_DEBUGGING_UNDER_PERFORMANCE_TESTING

// settings for the hashmap implementation
#define NUM_QUEUES_IN_BUCKETS 0 // simple hashmap without the "direct" buckets

#define IMPLEMENTATION_NAME_STRING "hashmap_no_wild_simple"
#define IMPLEMENTATION_NAME_FUNCTION_PREFIX hashmap_no_wild_simple

// include the actual implementation
#include "hashmap_matching_queue_impl.h"
