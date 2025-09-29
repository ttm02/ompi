
#ifndef OMPI_EVENT_COLLECTION_IMPL_H
#define OMPI_EVENT_COLLECTION_IMPL_H
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// collect the matching events

struct matching_event {
    int32_t tag;
    int32_t peer;
    int32_t is_recv;
};

struct matching_events {
    char* output_filename;
    int32_t num_communicators;
    void** communicators;
    struct matching_event** events;
    int32_t* event_count;
};

extern struct matching_events* events;

void init_events_filename()
{
    pid_t pid = getpid();

    // allocate buffer for filename
    char buf[64];
    snprintf(buf, sizeof(buf), "events_%d.bin", (int)pid);

    // duplicate string
    events->output_filename = strdup(buf);
    if (!events->output_filename) {
        perror("strdup");
        exit(EXIT_FAILURE);
    }
}

inline void write_events_to_file()
{
    if (events == NULL || events->num_communicators == 0) {
        //fprintf(stderr, "No events to write\n");
        return;
    }

    FILE *f = fopen(events->output_filename, "wb");
    if (!f) {
        perror("fopen");
        return;
    }

    // Write num_communicators
    fwrite(&events->num_communicators, sizeof(int32_t), 1, f);

    // For each communicator
    for (int i = 0; i < events->num_communicators; i++) {
        // Write event_count
        fwrite(&events->event_count[i], sizeof(int32_t), 1, f);
    }

    for (int i = 0; i < events->num_communicators; i++) {
        // Write all events for this communicator
        if (events->event_count[i] > 0) {
            fwrite(events->events[i],
                   sizeof(struct matching_event),
                   events->event_count[i],
                   f);
        }
    }

    fclose(f);
}


inline void add_event(void* communicator, int tag,int peer,int is_recv)
{
    if (events==NULL) {
        events = calloc(sizeof(struct matching_events),1);
        init_events_filename();
    }

    int communicator_idx=-1;
    // find communicator
    for (int i = 0; i < events->num_communicators; ++i) {
        if (events->communicators[i]==communicator) {
            communicator_idx=i;
            break;
        }
    }
    if (communicator_idx==-1){
    // add new communicator
        communicator_idx=events->num_communicators;

        events->communicators = realloc(events->communicators,sizeof(void*)*(communicator_idx+1));
        events->communicators[communicator_idx] = communicator;
        events->event_count= realloc(events->event_count,sizeof(int32_t)*(communicator_idx+1));
        events->event_count[communicator_idx]=0;
        events->num_communicators++;
        events->events =realloc(events->events,sizeof(struct matching_event*)*(communicator_idx+1));
        events->events[communicator_idx]=NULL;
    }
    int event_idx = events->event_count[communicator_idx];

    events->events[communicator_idx] = realloc(events->events[communicator_idx],sizeof(struct matching_event)*(event_idx+1));
    events->events[communicator_idx][event_idx].tag = tag;
    events->events[communicator_idx][event_idx].peer = peer;
    events->events[communicator_idx][event_idx].is_recv = is_recv;


    write_events_to_file();
}

inline void add_recv_event(void* communicator, int tag,int peer)
{
    add_event(communicator,tag,peer,1);
}

inline void add_msg_arrive_event(void* communicator, int tag,int peer)
{
    add_event(communicator,tag,peer,0);
}

inline void read_events_from_file(const char *filename)
{
    FILE *f = fopen(filename, "rb");
    if (!f) {
        perror("fopen");
        return;
    }

    assert(events == NULL);

    events = calloc(1, sizeof(struct matching_events));
    if (!events) {
        fclose(f);
        perror("calloc");
        return;
    }

    // Read num_communicators
    int32_t num_comms;
    if (fread(&num_comms, sizeof(int32_t), 1, f) != 1) {
        perror("fread num_communicators");
        fclose(f);
        return;
    }
    events->num_communicators = num_comms;

    // Allocate arrays
    events->event_count   = calloc(num_comms, sizeof(int));
    events->events        = calloc(num_comms, sizeof(struct matching_event*));
    events->communicators = calloc(num_comms, sizeof(void*)); // will remain NULL

    // Read event_count array
    if (fread(events->event_count, sizeof(int32_t), num_comms, f) != (size_t)num_comms) {
        perror("fread event_count");
        fclose(f);
        return;
    }

    // Read events for each communicator
    for (int i = 0; i < num_comms; i++) {
        int count = events->event_count[i];
        if (count > 0) {
            events->events[i] = malloc(sizeof(struct matching_event) * count);
            if (!events->events[i]) {
                perror("malloc events");
                fclose(f);
                return;
            }
            if (fread(events->events[i], sizeof(struct matching_event), count, f) != (size_t)count) {
                perror("fread events");
                fclose(f);
                return;
            }
        } else {
            events->events[i] = NULL;
        }
    }

    fclose(f);
}





#endif // OMPI_EVENT_COLLECTION_IMPL_H
