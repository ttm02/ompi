
#define OUTSIDE_CONFIGURATION
#define NO_DEBUGGING_UNDER_PERFORMANCE_TESTING

// settings for the hashmap implementation
#define WILDCARD_SUPPORT
#define WILDCARD_NO_OVERTAKE_SUPPORT

#define IMPLEMENTATION_NAME_STRING "hashmap_wild_full"
#define IMPLEMENTATION_NAME_FUNCTION_PREFIX hashmap_wild_full

// include the actual implementation
#include "hashmap_matching_queue_impl.h"