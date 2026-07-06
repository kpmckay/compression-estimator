#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <stdint.h>
#include <pthread.h>
#include <ftw.h>
#include <math.h>

#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>

#include "zlib.h"

#define HISTOGRAM_RESOLUTION     128
#define GZIP_COMPRESSION_LEVEL   1

uint64_t buckets[4096+1] = {0};
uint64_t padding_buckets[4096+1] = {0};
uint64_t status = 0;
int32_t  threadpool;
uint32_t thread_cnt = 1;
uint64_t dirmode_bytes = 0;
uint64_t dirmode_files = 0;
uint8_t  quantized_compression = 0;
uint8_t  measure_entropy = 0;
uint8_t  measure_padding = 0;
uint8_t  detect_file_headers = 0;
uint64_t symbol_table[256] = {0};

pthread_mutex_t bucket_mutex;
pthread_mutex_t padding_mutex;
pthread_mutex_t status_mutex;
pthread_mutex_t threadpool_mutex;
pthread_mutex_t symbol_table_mutex;

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
   run = 0;
}

enum {
   FMT_UNKNOWN = 0,
   FMT_7Z, FMT_AAC, FMT_AVI, FMT_AVIF, FMT_BPG, FMT_BZ2,
   FMT_FLAC, FMT_FLV, FMT_GIF, FMT_GZIP, FMT_HEIF, FMT_JPEG,
   FMT_JPEG2000, FMT_LZ4, FMT_MKV, FMT_MOV, FMT_MP3, FMT_MP4,
   FMT_MPEG, FMT_OGG, FMT_PNG, FMT_RAR, FMT_TIFF, FMT_WAV,
   FMT_WEBP, FMT_WMA, FMT_XZ, FMT_ZIP, FMT_ZSTD,
   FMT_COUNT
};

typedef struct {
   const char* name;
   uint64_t sig_count;
   uint64_t byte_count;
} format_t;

format_t formats[FMT_COUNT] = {
   [FMT_UNKNOWN]  = {"unknown"},
   [FMT_7Z]       = {"7z"},
   [FMT_AAC]      = {"aac"},
   [FMT_AVI]      = {"avi"},
   [FMT_AVIF]     = {"avif"},
   [FMT_BPG]      = {"bpg"},
   [FMT_BZ2]      = {"bz2"},
   [FMT_FLAC]     = {"flac"},
   [FMT_FLV]      = {"flv"},
   [FMT_GIF]      = {"gif"},
   [FMT_GZIP]     = {"gzip"},
   [FMT_HEIF]     = {"heif"},
   [FMT_JPEG]     = {"jpeg"},
   [FMT_JPEG2000] = {"jpeg2000"},
   [FMT_LZ4]      = {"lz4"},
   [FMT_MKV]      = {"mkv"},
   [FMT_MOV]      = {"mov"},
   [FMT_MP3]      = {"mp3"},
   [FMT_MP4]      = {"mp4"},
   [FMT_MPEG]     = {"mpeg"},
   [FMT_OGG]      = {"ogg"},
   [FMT_PNG]      = {"png"},
   [FMT_RAR]      = {"rar"},
   [FMT_TIFF]     = {"tiff"},
   [FMT_WAV]      = {"wav"},
   [FMT_WEBP]     = {"webp"},
   [FMT_WMA]      = {"wma"},
   [FMT_XZ]       = {"xz"},
   [FMT_ZIP]      = {"zip"},
   [FMT_ZSTD]     = {"zstd"},
};

int detect_compression(int fd, int64_t size) {

   unsigned char signature[16] = {0};
   ssize_t bytes_read;
   int fmt = FMT_UNKNOWN;

   bytes_read = pread(fd, signature, sizeof(signature), 0);
   if (bytes_read < 0)
      return EXIT_FAILURE;

   if (bytes_read >= 6 && memcmp(signature, "\xFD\x37\x7A\x58\x5A\x00", 6) == 0)
      fmt = FMT_XZ;
   else if (bytes_read >= 4 && memcmp(signature, "\x04\x22\x4D\x18", 4) == 0)
      fmt = FMT_LZ4;
   else if (bytes_read >= 4 && memcmp(signature, "\x28\xB5\x2F\xFD", 4) == 0)
      fmt = FMT_ZSTD;
   else if (bytes_read >= 3 && memcmp(signature, "\x42\x5A\x68", 3) == 0)
      fmt = FMT_BZ2;
   else if (bytes_read >= 2 && memcmp(signature, "\x1F\x8B", 2) == 0)
      fmt = FMT_GZIP;
   else if (bytes_read >= 7 && (memcmp(signature, "\x52\x61\x72\x21\x1A\x07\x00", 7) == 0 ||
                                memcmp(signature, "\x52\x61\x72\x21\x1A\x07\x01", 7) == 0))
      fmt = FMT_RAR;
   else if (bytes_read >= 6 && memcmp(signature, "\x37\x7A\xBC\xAF\x27\x1C", 6) == 0)
      fmt = FMT_7Z;
   else if (bytes_read >= 4 && memcmp(signature, "\x50\x4B\x03\x04", 4) == 0)
      fmt = FMT_ZIP;
   else if (bytes_read >= 4 && memcmp(signature, "\x66\x4C\x61\x43", 4) == 0)
      fmt = FMT_FLAC;
   else if (bytes_read >= 4 && memcmp(signature, "\x46\x4C\x56\x01", 4) == 0)
      fmt = FMT_FLV;
   else if (bytes_read >= 4 && memcmp(signature, "\x1A\x45\xDF\xA3", 4) == 0)
      fmt = FMT_MKV;
   else if (bytes_read >= 12 && memcmp(signature, "\x52\x49\x46\x46", 4) == 0 &&
                                memcmp(signature + 8, "\x57\x41\x56\x45", 4) == 0)
      fmt = FMT_WAV;
   else if (bytes_read >= 12 && memcmp(signature, "\x52\x49\x46\x46", 4) == 0 &&
                                memcmp(signature + 8, "\x41\x56\x49\x20", 4) == 0)
      fmt = FMT_AVI;
   else if (bytes_read >= 2 && (memcmp(signature, "\xFF\xF1", 2) == 0 ||
                                memcmp(signature, "\xFF\xF9", 2) == 0))
      fmt = FMT_AAC;
   else if (bytes_read >= 2 && signature[0] == 0xFF && (signature[1] & 0xF0) == 0xF0)
      fmt = FMT_MP3;
   else if (bytes_read >= 8 && memcmp(signature, "\x89\x50\x4E\x47\x0D\x0A\x1A\x0A", 8) == 0)
      fmt = FMT_PNG;
   else if (bytes_read >= 8 && memcmp(signature, "\x00\x00\x00\x0C\x6A\x50\x20\x20", 8) == 0)
      fmt = FMT_JPEG2000;
   else if (bytes_read >= 12 && memcmp(signature, "\x52\x49\x46\x46", 4) == 0 &&
                                memcmp(signature + 8, "\x57\x45\x42\x50", 4) == 0)
      fmt = FMT_WEBP;
   else if (bytes_read >= 6 && memcmp(signature, "\x47\x49\x46\x38", 4) == 0 &&
                               (memcmp(signature + 4, "\x37\x61", 2) == 0 ||
                                memcmp(signature + 4, "\x39\x61", 2) == 0))
      fmt = FMT_GIF;
   else if (bytes_read >= 4 && (memcmp(signature, "\x4D\x4D\x00\x2A", 4) == 0 ||
                                memcmp(signature, "\x49\x49\x2A\x00", 4) == 0))
      fmt = FMT_TIFF;
   else if (bytes_read >= 12 && memcmp(signature + 4, "\x66\x74\x79\x70", 4) == 0 &&
                                (memcmp(signature + 8, "\x68\x65\x69\x63", 4) == 0 ||
                                 memcmp(signature + 8, "\x6D\x69\x66\x31", 4) == 0))
      fmt = FMT_HEIF;
   else if (bytes_read >= 2 && memcmp(signature, "\xFF\xD8", 2) == 0)
      fmt = FMT_JPEG;
   else if (bytes_read >= 4 && memcmp(signature, "\x30\x26\xB2\x75", 4) == 0)
      fmt = FMT_WMA;
   else if (bytes_read >= 4 && memcmp(signature, "\x4F\x67\x67\x53", 4) == 0)
      fmt = FMT_OGG;
   else if (bytes_read >= 12 && memcmp(signature + 4, "\x66\x74\x79\x70", 4) == 0 &&
                                memcmp(signature + 8, "\x71\x74\x20\x20", 4) == 0)
      fmt = FMT_MOV;
   else if (bytes_read >= 12 && memcmp(signature + 4, "\x66\x74\x79\x70", 4) == 0 &&
                                memcmp(signature + 8, "\x61\x76\x69\x66", 4) == 0)
      fmt = FMT_AVIF;
   else if (bytes_read >= 4 && memcmp(signature, "\x42\x50\x47\xFB", 4) == 0)
      fmt = FMT_BPG;
   else if (bytes_read >= 8 && memcmp(signature + 4, "\x66\x74\x79\x70", 4) == 0)
      fmt = FMT_MP4;
   else if (bytes_read >= 1 && signature[0] == 0x47)
      fmt = FMT_MPEG;

   formats[fmt].sig_count++;
   formats[fmt].byte_count += size;

   return EXIT_SUCCESS;
}

void* monitor_thread(void* t_ops) {

   monitor_thread_t ops = *((monitor_thread_t*) t_ops);
   uint64_t bytes_prev = 0;

   sleep(1);

   while (run) {

      pthread_mutex_lock(&status_mutex);
         if ( ops.dirmode ) {
            fprintf(stderr, "\r%.1f GiB Completed (%ld files) with %d threads active", (status*1.0) / (1024*1024*1024), \
            dirmode_files, threadpool);      
         } else {
            fprintf(stderr, "\r%.1f GiB Completed (%.1f%%) [%lu MiB/s]", (status*1.0) / (1024*1024*1024), \
            ((status*1.0) / ops.bytes)*100, (status - bytes_prev) / (1024*1024)); 
         }
         bytes_prev = status;
      pthread_mutex_unlock(&status_mutex);
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

   uint64_t local_buckets[4097] = {0};
   uint64_t local_padding[4097] = {0};
   uint64_t local_symbols[256] = {0};
   uint64_t local_status = 0;

   uint8_t* ibuff = aligned_alloc(4096, (128*1024));
   uint8_t* obuff = malloc((4096+1024)*sizeof(uint8_t));
   uint8_t* zbuff = calloc(4096, sizeof(uint8_t));

   if (!ibuff || !zbuff || !obuff) {
      fprintf(stderr, "Error allocating buffers\n");
      free(ibuff);
      free(obuff);
      free(zbuff);
      return NULL;
   }

   z_stream strm;
   strm.zalloc = Z_NULL;
   strm.zfree  = Z_NULL;
   strm.opaque = Z_NULL;

   deflateInit(&strm, GZIP_COMPRESSION_LEVEL);

   while (bytes && run) {

      bytes_to_pread = bytes_this_pass = (bytes >= (128*1024) ? (128*1024) : bytes);
      ssize_t total_read = 0;
      while (bytes_to_pread) {
         bytes_in_pread = pread(fd, ibuff+total_read, bytes_to_pread, offset+total_read);
         if (bytes_in_pread < 1) {
             fprintf(stderr, "Unable to read from disk at offset %lu: %s\n", offset+total_read, strerror(errno));
             break;
         }
         total_read += bytes_in_pread;
         bytes_to_pread -= bytes_in_pread;
      }

      if (total_read < bytes_this_pass)
         memset(ibuff+total_read, 0, bytes_this_pass - total_read);

      offset += bytes_this_pass;
      bytes  -= bytes_this_pass;

      if (measure_entropy) {
         for ( i = 0; i < bytes_this_pass; i++ )
            local_symbols[ibuff[i]]++;
      }

      for (i = 0; bytes_this_pass > 0 ; i++) {

         bytes_to_compress = (bytes_this_pass >= 4096 ? 4096 : bytes_this_pass);

         if (memcmp(zbuff, ibuff+(4096*i), 4096*sizeof(uint8_t)) == 0) {
            local_buckets[0]++;
         } else {
            strm.avail_in  = bytes_to_compress;
            strm.next_in   = ibuff+(4096*i);
            strm.avail_out = 4096+1024;
            strm.next_out  = obuff;

            if (deflate(&strm, Z_FINISH) != Z_STREAM_END) {
               fprintf(stderr, "An error occurred during zlib compression\n");
               run = 0;
            }

            strm.total_out > 3840 ? local_buckets[4096]++ : local_buckets[strm.total_out]++;

            deflateReset(&strm);
         }

         if (measure_padding) {
            int pad = 0;
            for (int p = bytes_to_compress - 1; p >= 0 && ibuff[(4096*i) + p] == 0; p--)
               pad++;
            if (pad >= HISTOGRAM_RESOLUTION)
               local_padding[pad]++;
         }

         local_status += bytes_to_compress;
         bytes_this_pass -= bytes_to_compress;
      }

      pthread_mutex_lock(&status_mutex);
         status += local_status;
      pthread_mutex_unlock(&status_mutex);
      local_status = 0;
   }

   deflateEnd(&strm);

   pthread_mutex_lock(&bucket_mutex);
      for (i = 0; i <= 4096; i++)
         buckets[i] += local_buckets[i];
   pthread_mutex_unlock(&bucket_mutex);

   if (measure_padding) {
      pthread_mutex_lock(&padding_mutex);
         for (i = 0; i <= 4096; i++)
            padding_buckets[i] += local_padding[i];
      pthread_mutex_unlock(&padding_mutex);
   }

   if (measure_entropy) {
      pthread_mutex_lock(&symbol_table_mutex);
         for (i = 0; i < 256; i++)
            symbol_table[i] += local_symbols[i];
      pthread_mutex_unlock(&symbol_table_mutex);
   }

   if (close_fd) {
      close(fd);
      free(t_ops);
      pthread_mutex_lock(&threadpool_mutex);
         threadpool--;
      pthread_mutex_unlock(&threadpool_mutex);
   }

   free(ibuff);
   free(zbuff);
   free(obuff);

   return NULL;
}

void print_results(void) {

   uint32_t i, j, k;
   uint64_t bucket_sum = 0;
   uint64_t quantized_bucket_sum = 0;
   uint64_t bucket_sum_uncompressed = 0;
   uint64_t bucket_tally = 0;
   uint64_t max_bucket_tally = 0;
   uint64_t total_symbols = 0;
   float    hash_count = 0;
   float    hash_percent = 0;
   float    compression_ratio = 0;
   float    quantized_compression_ratio = 0;
   float    entropy = 0;
   float    p_sym = 0;

   /* Get total (un)compressed bytes */ 
   k = 512;   // Minimum size in 512-byte quantized compression
   for (i = 1; i <= 4096; i++ ) {
      bucket_sum_uncompressed += buckets[i] * 4096;
      bucket_sum += buckets[i] * i;
      quantized_bucket_sum += buckets[i] * k; 
      if ( (i % 512) == 0 )
         k += 512;
   }

   if (bucket_sum == 0)   // Could be zero if program is terminated quickly 
      compression_ratio = 0.0;
   else {
      compression_ratio = ((bucket_sum_uncompressed*1.0)/(bucket_sum*1.0)); 
      quantized_compression_ratio = ((bucket_sum_uncompressed*1.0)/(quantized_bucket_sum*1.0)); 
   }

   if (measure_entropy) {
      for ( i = 0; i<256; i++ )
         total_symbols += symbol_table[i];
      for ( i = 0; i<256; i++ ) {
         p_sym = (float)symbol_table[i] / (float)total_symbols;
         if (p_sym > 0)
            entropy += -1.0 * p_sym * log2(p_sym);
      }
   }

   printf("\n\n");
   printf("Total Bytes Analyzed     : %lu\n", bucket_sum_uncompressed);
   if (dirmode_files) 
      printf("Total Files Analyzed     : %lu\n", dirmode_files);
   printf("All Zero (Empty) Sectors : %lu\n", buckets[0]);
   printf("Incompressible Sectors   : %lu\n", buckets[4096]);
   if (measure_entropy)
      printf("Shannon Entropy (8-bit)  : %.2f\n", entropy);
   if (measure_padding) {
      uint64_t total_padding_bytes = 0;
      for ( i = 0; i <= 4096; i++ )
         total_padding_bytes += padding_buckets[i] * i;
      printf("Trailing Zero Padding    : %lu bytes (%.2f%% of data analyzed)\n",
             total_padding_bytes,
             bucket_sum_uncompressed > 0 ? ((float)total_padding_bytes/bucket_sum_uncompressed)*100 : 0.0);
   }

   if (dirmode_files && detect_file_headers) {
      printf("\n");
      printf("File Signature Analysis:\n\n");
      printf("   %lu files without compression detected (%.2f %% of files, %.2f %% of data analyzed)\n",
                 formats[FMT_UNKNOWN].sig_count,
                 ((float)formats[FMT_UNKNOWN].sig_count/dirmode_files)*100,
                 ((float)formats[FMT_UNKNOWN].byte_count/bucket_sum_uncompressed)*100);
      printf("   Files with compression detected...\n\n");

      printf("   File Type : # of Files   [ %% of Data ]\n");
      printf("   --------------------------------------\n");
      for (int f = 1; f < FMT_COUNT; f++) {
         printf("   %9s : %-12lu [ %7.2f %% ]\n", formats[f].name, formats[f].sig_count,
                ((float)formats[f].byte_count/bucket_sum_uncompressed)*100);
      }
   }

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
      if (quantized_compression)
         printf("Estimated Compression Ratio with 512-byte Quantization: %.1f:1\n", quantized_compression_ratio);
      else
         printf("\nEstimated Compression Ratio with ScaleFlux: %.1f:1\n", compression_ratio);
   } else 
      printf("\nCompression ratio with ScaleFlux cannot be estimated because the drive is empty\n");

   if (measure_padding) {
      uint64_t pad_tally = 0;
      uint64_t max_pad_tally = 0;
      for (i = HISTOGRAM_RESOLUTION+1; i <= 4096; i++ ) {
         pad_tally += padding_buckets[i];
         if (i % HISTOGRAM_RESOLUTION == 0) {
            if (pad_tally > max_pad_tally)
               max_pad_tally = pad_tally;
            pad_tally = 0;
         }
      }
      if (max_pad_tally > 0) {
         printf("\nPadding Histogram (minimum %d trailing zero bytes):\n\n", HISTOGRAM_RESOLUTION);
         pad_tally = 0;
         for (i = HISTOGRAM_RESOLUTION+1; i <= 4096; i++ ) {
            pad_tally += padding_buckets[i];
            if (i % HISTOGRAM_RESOLUTION == 0) {
               printf("   <= %4u Bytes: ", i);
               hash_percent = (float) pad_tally / max_pad_tally;
               hash_count = hash_percent * 50;
               for (j = 0; j < (int) hash_count; j++)
                  printf("#");
               if ( hash_count < 1 )
                  printf("#");
               printf(" %lu\n", pad_tally);
               pad_tally = 0;
            }
         }
      }
   }

   printf("\n");
   return;
}

int compress_dir_callback(const char* path, const struct stat* st, int32_t flag, struct FTW *ftwbuf) {

   int32_t fd = -1;
   pthread_t thread_id;
   compress_thread_t* ops = NULL;

   switch (st->st_mode & S_IFMT) {
      case S_IFDIR:
      case S_IFCHR:
      case S_IFBLK:
      case S_IFIFO:
      case S_IFSOCK:
         return EXIT_SUCCESS;
   }

   ops = malloc(sizeof(compress_thread_t));
   if (!ops) {
      fprintf(stderr, "Could not allocate memory for threads\n");
      return EXIT_FAILURE;
   }

   fd = open(path, O_RDONLY);
   if (fd == -1) {
      fprintf(stderr, "Unable to open %s for reading (%s)\n", path, strerror(errno));
      goto cleanup;
   }

   if (detect_file_headers) {
      if (detect_compression(fd, st->st_size)) {
         fprintf(stderr, "Unable to read file header from %s (%s)\n", path, strerror(errno));
         goto cleanup;
      }
   }

   ops->fd       = fd;
   ops->close_fd = 1;
   ops->bytes    = st->st_size;
   ops->offset   = 0;

   while (1) {
      pthread_mutex_lock(&threadpool_mutex);
      if (threadpool < thread_cnt)
         break;
      pthread_mutex_unlock(&threadpool_mutex);
      usleep(1000);
   }
   pthread_create(&thread_id, NULL, &compress_thread, ops);
   threadpool++;
   pthread_detach(thread_id);
   pthread_mutex_unlock(&threadpool_mutex);

   pthread_mutex_lock(&status_mutex);
      dirmode_bytes += ops->bytes;
      dirmode_files += 1;
   pthread_mutex_unlock(&status_mutex);

   return 0;

cleanup:
   free(ops);
   if (fd != -1)
      close(fd);
   return EXIT_FAILURE;
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

   while (1) {
      pthread_mutex_lock(&threadpool_mutex);
      if (threadpool <= 0) {
         pthread_mutex_unlock(&threadpool_mutex);
         break;
      }
      pthread_mutex_unlock(&threadpool_mutex);
      usleep(10000);
   }

   run = 0;   // Trigger to exit monitoring thread
   pthread_join(monitor_thread_id, NULL); 

   print_results();

   return EXIT_SUCCESS;
}

int compress_blk_or_file(char* path, struct stat st, int32_t isblk) {

   uint32_t i;
   int32_t fd;

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
      if (ioctl(fd, BLKGETSIZE64, &size_in_bytes) == -1) {
         fprintf(stderr, "Unable to get block device size (%s)\n", strerror(errno));
         close(fd);
         return EXIT_FAILURE;
      }
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

   if (!isblk && detect_file_headers) {
      if (detect_compression(fd, st.st_size)) {
         fprintf(stderr, "Unable to read file header from %s (%s)\n", path, strerror(errno));
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
      free(thread_id);
      free(ops);
      close(fd);
      return EXIT_FAILURE;
   }

   for(i = 0; i < thread_cnt; i++) {

      ops[i].fd       = fd;
      ops[i].close_fd = 0;
      ops[i].bytes    = bytes_per_thread;
      ops[i].offset   = bytes_per_thread * i;

      /* Add any rounding error to the last thread */
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

   print_results();

   free(thread_id);
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
      fprintf(stderr, "Advanced flags:\n\n");
      fprintf(stderr, "   -q : Estimate using 512-byte quantized compression\n");
      fprintf(stderr, "   -e : Measure data entropy\n");
      fprintf(stderr, "   -c : Show statistics for pre-compressed file formats (file or direcory mode only)\n");
      fprintf(stderr, "   -p : Measure trailing zero padding per 4kB block\n\n");
      exit(EXIT_FAILURE);
   } else {
      while ((args = getopt(argc, argv, "d:t:qecp")) != -1) {
         switch (args) {
            case 'd':   // File, directory or disk to test
               path = optarg;
               break;
            case 't':   // Number of compression threads
               thread_cnt = atoi(optarg);
               if (thread_cnt < 1)
                  thread_cnt = 1;
               break;
            case 'q':
               quantized_compression = 1;
               break;
            case 'e':
               measure_entropy = 1;
               break;
            case 'c':
               detect_file_headers = 1;
               break;
            case 'p':
               measure_padding = 1;
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
      fprintf(stderr, "Path specified is null\n");
      exit(EXIT_FAILURE);
   }

   pthread_mutex_init(&bucket_mutex, NULL);
   pthread_mutex_init(&padding_mutex, NULL);
   pthread_mutex_init(&status_mutex, NULL);
   pthread_mutex_init(&threadpool_mutex, NULL);
   pthread_mutex_init(&symbol_table_mutex, NULL);

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

   pthread_mutex_destroy(&bucket_mutex);
   pthread_mutex_destroy(&padding_mutex);
   pthread_mutex_destroy(&status_mutex);
   pthread_mutex_destroy(&threadpool_mutex);
   pthread_mutex_destroy(&symbol_table_mutex);

   exit(EXIT_SUCCESS);
}
