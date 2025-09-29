
#define OUTSIDE_CONFIGURATION
#define NO_DEBUGGING_UNDER_PERFORMANCE_TESTING

// settings for the hashmap implementation
#define BRANCHLESS_BUCKET_SELECTOR
#define ADAPT_BUCKETS

#define IMPLEMENTATION_NAME_STRING "hashmap_no_wild_branchless"
#define IMPLEMENTATION_NAME_FUNCTION_PREFIX hashmap_no_wild_branchless


// include the actual implementation
#include "hashmap_matching_queue_impl.h"
