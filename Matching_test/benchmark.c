/*
 * multithreaded_mpi_match_benchmark.c
 * Benchmark for MPI_THREAD_MULTIPLE message matching using OpenMP
 *
 * Each OpenMP thread alternately posts nonblocking receives and sends
 * to randomly chosen peers and tags. Measures throughput.
 *
 * Usage: mpirun -np <P> ./a.out -t <threads> -n <iterations> -s <msg_size>
 */
#include <mpi.h>
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static int world_size, rank;
static int nthreads = 4;
static long iterations = 1000;
static int msg_size = 64;

static double diff_time(double start, double end) {
    return end - start;
}

int main(int argc, char **argv) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI_THREAD_MULTIPLE not provided\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int opt;
    while ((opt = getopt(argc, argv, "t:n:s:")) != -1) {
        switch (opt) {
            case 't': nthreads = atoi(optarg); break;   
            case 'n': iterations = atol(optarg); break;
            case 's': msg_size = atoi(optarg); break;
            default:
                if (rank == 0)
                    fprintf(stderr, "Usage: %s [-t threads] [-n iterations] [-s msg_size]\n", argv[0]);
                MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    omp_set_num_threads(nthreads);

    // Allocate buffers
    char *sendbuf = malloc(msg_size * nthreads*2);
    char *recvbuf = malloc(msg_size * nthreads*2);


// communication partners
        int next = (rank + 1) % world_size;
        int prev = (rank - 1 + world_size) % world_size;// + world size to avoid negative
//        printf("Rank %d with %d and %d\n",rank,next,prev);
  
    // Synchronize before starting
//    MPI_Barrier(MPI_COMM_WORLD);
//TODO barrier leads to wildcard recv???

    double t0 = MPI_Wtime();
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        char *sbuf = sendbuf + tid * msg_size;
        char *sbuf2 = sendbuf + tid * msg_size +msg_size * nthreads;
        char *rbuf = recvbuf + tid * msg_size;
        char *rbuf2 = recvbuf + tid * msg_size +msg_size * nthreads;
        MPI_Request reqs[4];
        MPI_Status stats[4];
//        int tag  = tid; // thread pair wise communication
            int tag  = 0; // use same tags regardless of threads

        for (long i = 0; i < iterations; ++i) {
// halo exchange like benchmark
// next process
            MPI_Irecv(rbuf, msg_size, MPI_BYTE, next, tag, MPI_COMM_WORLD, &reqs[0]);
            MPI_Isend(sbuf, msg_size, MPI_BYTE, next, tag, MPI_COMM_WORLD, &reqs[1]);
// previous process
            MPI_Irecv(rbuf2, msg_size, MPI_BYTE, prev, tag, MPI_COMM_WORLD, &reqs[2]);
            MPI_Isend(sbuf2, msg_size, MPI_BYTE, prev, tag, MPI_COMM_WORLD, &reqs[3]);

            MPI_Waitall(4, reqs, stats);
        }
    }

    // Synchronize after completion
//    MPI_Barrier(MPI_COMM_WORLD);
     double t1 = MPI_Wtime();

    if (rank == 0) {
        double elapsed = diff_time(t0, t1);
        double total_ops = (double)iterations * nthreads;
        printf("Threads=%d, iters=%ld per thread, msg_size=%d bytes: Time=%.6f s, Throughput=%.3f Mops/s\n",
               nthreads, iterations, msg_size, elapsed, total_ops / elapsed / 1e6);
    }

    free(sendbuf);
    free(recvbuf);
    MPI_Finalize();
    return 0;
}

