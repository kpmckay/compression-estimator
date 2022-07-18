#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <stdint.h>
#include <malloc.h>
#include <pthread.h>
#include <dirent.h>
#include <ftw.h>

#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>

#include "zlib.h"

#define HISTOGRAM_RESOLUTION     128
#define GZIP_COMPRESSION_LEVEL   1

extern int errno;

uint64_t buckets[4096+1] = {0};
uint64_t status = 0;
int32_t  threadpool;
uint32_t thread_cnt = 1;
uint64_t dirmode_bytes = 0;
uint64_t dirmode_files = 0;

pthread_mutex_t bucket_mutex;
pthread_mutex_t status_mutex;
pthread_mutex_t threadpool_mutex;
pthread_mutex_t threadpool_create_mutex;

typedef struct {
   int32_t fd;
   int32_t close_fd;
   int64_t offset;
   int64_t bytes;
} compress_thread_t;

typedef struct {
   int64_t bytes;
   int32_t dirmode;
} monitor_thread_t;

static volatile sig_atomic_t run = 1;
static void signal_handler(int _) {
   (void)_;
   fprintf(stderr, "\rTerminating on ctrl-c...\n");
   run = 0;
}

void* monitor_thread(void* t_ops) {

   monitor_thread_t ops = *((monitor_thread_t*) t_ops);
   uint64_t bytes_prev = 0;

   sleep(1);

   while (run) {

      if ( ops.dirmode ) {
         fprintf(stderr, "\r%.1f GiB Completed (%ld files) with %d threads active", (status*1.0) / (1024*1024*1024), \
         dirmode_files, threadpool);      
      } else {
         fprintf(stderr, "\r%.1f GiB Completed (%.1f%%) [%lu MiB/s]", (status*1.0) / (1024*1024*1024), \
         ((status*1.0) / ops.bytes)*100, (status - bytes_prev) / (1024*1024)); 
      }

      bytes_prev = status;
      sleep(1);
   }
   return NULL;
}


void* compress_thread(void* t_ops) {

   compress_thread_t ops = *((compress_thread_t*) t_ops);

   uint32_t i;

   ssize_t bytes_this_pass;
   ssize_t bytes_to_pread;
   ssize_t bytes_in_pread;
   ssize_t bytes_to_compress;

   int32_t fd       = ops.fd;   
   int32_t close_fd = ops.close_fd;   
   int64_t offset   = ops.offset;
   int64_t bytes    = ops.bytes;

   uint8_t* ibuff = memalign(4096, (128*1024));            // 128kB buffer, 4k aligned for pread 
   uint8_t* obuff = malloc((4096+1024)*sizeof(uint8_t));   // 4kB+1kB ouptut buffer (compressed size can be larger) 
   uint8_t* zbuff = calloc(4096, sizeof(uint8_t));         // 4kB buffer, initialized to zero 

   if (!ibuff || !zbuff || !obuff) {
      fprintf(stderr, "Error allocating buffers\n");
      return NULL;
   }

   z_stream strm;
   strm.zalloc = Z_NULL;
   strm.zfree  = Z_NULL;
   strm.opaque = Z_NULL;

   deflateInit(&strm, GZIP_COMPRESSION_LEVEL);

   while (bytes && run) {

      memset(ibuff,0,(128*1024));

      // Break up preads into 128kB units (if we need to read more than 128kB)
      bytes_to_pread = bytes_this_pass = (bytes >= (128*1024) ? (128*1024) : bytes);

      bytes_in_pread = 0;
      while (bytes_to_pread) {
         bytes_in_pread = pread(fd, ibuff+bytes_in_pread, bytes_to_pread, offset+bytes_in_pread);
         if (bytes_in_pread < 1) {
             fprintf(stderr, "Unable to read from disk at offset %lu: %s\n", offset+i, strerror(errno));
             break;
         }
         bytes_to_pread -= bytes_in_pread;
      }

      offset += bytes_this_pass;
      bytes  -= bytes_this_pass;

      for (i = 0; bytes_this_pass > 0 ; i++) {

         /* Compress in 4k units to match hardware compression unit */
         bytes_to_compress = (bytes_this_pass >= 4096 ? 4096 : bytes_this_pass);   

         /* Check if the data read is all zero, if so, skip compression */
         if (memcmp(zbuff, ibuff+(4096*i), 4096*sizeof(uint8_t)) == 0) {
            pthread_mutex_lock(&bucket_mutex);
               buckets[0]++;
            pthread_mutex_unlock(&bucket_mutex);
         } else {
            /* Compress 4k unit */
            strm.avail_in  = bytes_to_compress; 
            strm.next_in   = ibuff+(4096*i);
            strm.avail_out = 4096+1024; // Length of obuff
            strm.next_out  = obuff;

            if (deflate(&strm, Z_FINISH) != Z_STREAM_END) {
               fprintf(stderr, "An error occurred during zlib compression\n");
               run = 0;
            }

            pthread_mutex_lock(&bucket_mutex);
               // Simulate compression bypass
               strm.total_out > 3840 ? buckets[4096]++ : buckets[strm.total_out]++;
            pthread_mutex_unlock(&bucket_mutex);

            deflateReset(&strm);
         }

         pthread_mutex_lock(&status_mutex);
            status+=bytes_to_compress;
         pthread_mutex_unlock(&status_mutex);

         bytes_this_pass -= bytes_to_compress;
      }
   }

   deflateEnd(&strm);

   if (ibuff)
      free(ibuff);
   if (zbuff)
      free(zbuff);
   if (obuff)
      free(obuff);

   /* When called from directory processing mode, close the file descriptor and 
      add back to the threadpool */
   if (close_fd) {
      close(fd);
      pthread_mutex_lock(&threadpool_mutex);
         threadpool--;
      pthread_mutex_unlock(&threadpool_mutex);
   }

   return NULL;
}

void print_results(uint64_t size_in_bytes) {

   uint32_t i, j;
   uint64_t bucket_sum = 0;
   uint64_t bucket_sum_uncompressed = 0;
   uint64_t bucket_tally = 0;
   uint64_t max_bucket_tally = 0;
   float    hash_count = 0;
   float    hash_percent = 0;
   float    compression_ratio = 0;

   /* Get total (un)compressed bytes */ 
   for (i = 1; i <= 4096; i++ ) {

      /* Byte sums */
      bucket_sum += buckets[i] * i;
      bucket_sum_uncompressed += buckets[i] * 4096;
   }

   if (bucket_sum == 0)   // Could be zero if program is terminated quickly 
      compression_ratio = 0.0;
   else
      compression_ratio = ((bucket_sum_uncompressed*1.0)/(bucket_sum*1.0)); 

   printf("\n\n");
   printf("Total Bytes Analyzed     : %lu\n", bucket_sum_uncompressed);
   if (dirmode_files)
   printf("Total Files Analyzed     : %lu\n", dirmode_files);
   printf("All Zero (Empty) Sectors : %lu\n", buckets[0]);
   printf("Incompressible Sectors   : %lu\n", buckets[4096]);

   /* Get the histogram entry with the biggest value */
   for (i = 1; i <= 4096; i++ ) {
      bucket_tally += buckets[i];
      if (i % HISTOGRAM_RESOLUTION == 0) {
         if (bucket_tally > max_bucket_tally)
            max_bucket_tally = bucket_tally;
         bucket_tally = 0;
      }
   }

   /* Print the histogram */
   if(max_bucket_tally > 0) {   // Could be zero if drive is empty
      printf("\nCompressibility Histogram:\n\n");
      bucket_tally = 0;
      for (i = 1; i <= 4096; i++ ) {
         bucket_tally += buckets[i];
         if (i % HISTOGRAM_RESOLUTION == 0) {
            printf("   <= %4u Bytes: ", i);
            hash_percent = (float) bucket_tally / max_bucket_tally;
            hash_count = hash_percent * 50;
            for (j = 0; j < (int) hash_count; j++) {
               printf("#");
            }
            if ( hash_count < 1 )
               printf("#");
            printf(" %lu\n", bucket_tally);
            bucket_tally = 0;
         }
      }  
      printf("\nEstimated Compression Ratio with ScaleFlux: %.1f:1\n", compression_ratio);
   } else 
      printf("\nCompression ratio with ScaleFlux cannot be estimated because the drive is empty\n");

   printf("\n");
   return;
}


int compress_dir_callback(const char* path, const struct stat* st, int32_t flag, struct FTW *ftwbuf) {

   int32_t fd;
    
   pthread_t* thread_id;
   compress_thread_t* ops;

   /* Skip special files */
   switch (st->st_mode & S_IFMT) {
      case S_IFDIR:
      case S_IFCHR:
      case S_IFBLK:
      case S_IFIFO:
      case S_IFSOCK:
         return EXIT_SUCCESS;
   }

   thread_id = malloc(thread_cnt*sizeof(pthread_t));
   ops       = malloc(thread_cnt*sizeof(compress_thread_t));

   if (!thread_id || !ops) {
      fprintf(stderr, "Could not allocate memory for threads\n");
      return EXIT_FAILURE;
   }

   fd = open(path, O_RDONLY);   // Read-only

   if (fd == -1) {
      fprintf(stderr, "Unable to open %s for reading (%s)\n", path, strerror(errno));
      return 0;
   }

   ops->fd       = fd;
   ops->close_fd = 1;   // Request compression thread to close the file descriptor
   ops->bytes    = st->st_size;   // Compress entire file
   ops->offset   = 0; 

   /* Wait until there is room in the threadpool */ 
   while (threadpool >= thread_cnt)
      usleep(1000);

   /* Take a credit from the threadpool and launch a thread */
   pthread_mutex_lock(&threadpool_mutex);
      threadpool++;
      pthread_create(thread_id, NULL, &compress_thread, ops);
      pthread_detach(*thread_id);
   pthread_mutex_unlock(&threadpool_mutex);

   /* Increment counters */
   dirmode_bytes += ops->bytes;
   dirmode_files += 1;

   return 0;
}

int compress_dir(char* path, struct stat st) {

   pthread_t monitor_thread_id;
   monitor_thread_t m_ops;

   m_ops.bytes = 0; 
   m_ops.dirmode = 1; 

   pthread_create(&monitor_thread_id, NULL, &monitor_thread, &m_ops);

   if (nftw(path, compress_dir_callback, 64, 0) == -1) {
      fprintf(stderr, "Could not traverse directory %s\n", path);
      return EXIT_FAILURE;
   }

   sleep(2);

   run = 0;   // Trigger to exit monitoring thread
   pthread_join(monitor_thread_id, NULL); 

   print_results(dirmode_bytes);

   return EXIT_SUCCESS;
}

int compress_blk_or_file(char* path, struct stat st, int32_t isblk) {

   uint32_t i;
   uint32_t fd;

   int64_t size_in_bytes = 0;
   int64_t bytes_per_thread = 0;
   int64_t roundoff = 0;

   pthread_t* thread_id;
   pthread_t  monitor_thread_id;

   compress_thread_t* ops;
   monitor_thread_t   m_ops;

   if (isblk) {
      fd = open(path, O_RDONLY|O_DIRECT);   // Read-only, Direct IO
   } else {
      fd = open(path, O_RDONLY);   // Read-only
   }

   if (fd == -1) {
      fprintf(stderr, "Unable to open %s for reading (%s)\n", path, strerror(errno));
      return EXIT_FAILURE;
   }

   if (isblk) {
      ioctl(fd, BLKGETSIZE64, &size_in_bytes); 
   } else {
      size_in_bytes = st.st_size;
   }

   /* Validate returned file size */
   if (size_in_bytes < 1) {
      fprintf(stderr, "No bytes to read (%s)\n", strerror(errno));
      close(fd);
      return EXIT_FAILURE;
   }
   if (isblk) {
      if (size_in_bytes % 512 != 0) {
         fprintf(stderr, "Returned disk size is not in 512-byte units\n");
         close(fd);
         return EXIT_FAILURE;
      }
   }

   /* Just use a singe thread for small files or block devices */
   if (size_in_bytes / thread_cnt < 4096) {
      thread_cnt = 1;
      bytes_per_thread = size_in_bytes;
   } else {
      bytes_per_thread = (size_in_bytes - (size_in_bytes % 4096));   // Round down to multiple of 4k
      bytes_per_thread = bytes_per_thread / 4096 ;                   // Temporarily convert to 4k units
      bytes_per_thread = bytes_per_thread / thread_cnt;              // Divide 4k units into threads
      bytes_per_thread = bytes_per_thread * 4096 ;                   // Convert back to bytes
   }

   /* There could be a small remainder from the above calculation */
   roundoff = ((size_in_bytes - (bytes_per_thread * thread_cnt)));

   thread_id = malloc(thread_cnt*sizeof(pthread_t));
   ops       = malloc(thread_cnt*sizeof(compress_thread_t));

   if (!thread_id || !ops) {
      fprintf(stderr, "Could not allocate memory for threads\n");
      return EXIT_FAILURE;
   }

   for(i = 0; i < thread_cnt; i++) {

      ops[i].fd       = fd;
      ops[i].close_fd = 0;
      ops[i].bytes    = bytes_per_thread;
      ops[i].offset   = bytes_per_thread * i;

      /* Add any rounding error to the LAST thread */
      if (i == thread_cnt-1)
         ops[i].bytes += roundoff;

      pthread_create(&thread_id[i], NULL, &compress_thread, &ops[i]);
   }   

   m_ops.bytes = size_in_bytes; 
   m_ops.dirmode = 0;
   pthread_create(&monitor_thread_id, NULL, &monitor_thread, &m_ops);

   for(i = 0; i < thread_cnt; i++) {
      pthread_join(thread_id[i], NULL);
   }

   run = 0;   // Trigger to exit monitoring thread
   pthread_join(monitor_thread_id, NULL); 

   print_results(size_in_bytes);

   if (thread_id)
      free(thread_id);  
   if (ops)
      free(ops);  
   close(fd);

   return EXIT_SUCCESS;
} 


int main(int argc, char* argv[]) {

   int32_t  args;
   char*    path = NULL;
   struct   stat path_stat;

   if (argc <= 2) {
      fprintf(stderr, "Reads an entire disk, single file, or directory (recursively)");
      fprintf(stderr, " and estimates compressibility on ScaleFlux devices.\n\n");
      fprintf(stderr, "\tUsage: %s -d <File, Directory, or Block Device> -t <Threads>\n\n", argv[0]);
      exit(EXIT_FAILURE);
   } else {
      while ((args = getopt(argc, argv, "d:t:")) != -1) {
         switch (args) {
            case 'd':   // File, directory or disk to test
               path = optarg;
               break;
            case 't':   // Number of compression threads
               thread_cnt = atoi(optarg);
               break;
            case '?':
               fprintf(stderr, "Unknown option %c\n", optopt);
            default:
               fprintf(stderr, "Usage: %s -d <File, Directory, or Block Device> -t <Threads>\n\n", argv[0]);
               exit(EXIT_FAILURE);
         }
      }
   }

   if ( path == NULL ) {
      fprintf(stderr, "Usage: %s -d <File, Directory, or Block Device> -t <Threads>\n\n", argv[0]);
      exit(EXIT_FAILURE);
   }

   signal(SIGINT, signal_handler);   // Intercept ctrl-c

   // Determine if path leads to a file, directory, or block device
   if (stat(path, &path_stat) == 0) {
      switch (path_stat.st_mode & S_IFMT) {
         case S_IFCHR:
         case S_IFIFO:
         case S_IFSOCK:
            fprintf(stderr, "Could not open path %s as a file, directory, or block device\n", path);
            exit(EXIT_FAILURE);
            break;
         case S_IFDIR:
            fprintf(stderr, "Processing %s as a directory using %d threads\n", path, thread_cnt);
            compress_dir(path, path_stat);
            break;
         case S_IFBLK:  
            fprintf(stderr, "Processing %s as a block device using %d threads\n", path, thread_cnt);
            compress_blk_or_file(path, path_stat, 1);
            break;
         default:
            fprintf(stderr, "Processing %s as a file using %d threads\n", path, thread_cnt);
            compress_blk_or_file(path, path_stat, 0);
      }
   } else {
      fprintf(stderr, "Could not open path %s (ERRNO: %s)\n", path, strerror(errno));
      exit(EXIT_FAILURE);
   }

   exit(EXIT_SUCCESS);
}
