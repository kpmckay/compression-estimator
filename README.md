## Building
```
gcc c_est.c -pthread -D_GNU_SOURCE -lz -o c_est
```
 
## Usage
```
./c_est -d <drive_or_partition> -t <number_of_threads>
```
 
## Decription
This utility scans any block device and estimates the compression ratio that 
would be achieved on a ScaleFlux device with transparent compression. As such, 
the utility compresses on 4KB boundaries and emulates compression bypass so that
the data size does not grow for incompressible data. The number of threads
strongly influences performance as per-core zlib is fairly slow, but with a
sufficiently high number of threads, throughput can saturate Gen3 x4.

## Sample Output (Block Device Scan)
```
$ sudo ./c_est -d /dev/sfdv0n1 -t 64
Processing /dev/sfdv0n1 as a block device using 64 threads
2980.8 GiB Completed (100.0%) [61 MiB/s]]

Total Bytes Analyzed     : 335162269696
All Zero (Empty) Sectors : 699577520
Incompressible Sectors   : 0

Compressibility Histogram:

   <=  128 Bytes: ## 4031268
   <=  256 Bytes: # 8607
   <=  384 Bytes: # 8001
   <=  512 Bytes: # 9698
   <=  640 Bytes: # 13737
   <=  768 Bytes: # 19039
   <=  896 Bytes: # 33533
   <= 1024 Bytes: # 87265
   <= 1152 Bytes: # 347926
   <= 1280 Bytes: # 887311
   <= 1408 Bytes: # 2804164
   <= 1536 Bytes: ################################################## 73545776
   <= 1664 Bytes: # 30401
   <= 1792 Bytes: # 0
   <= 1920 Bytes: # 0
   <= 2048 Bytes: # 0
   <= 2176 Bytes: # 0
   <= 2304 Bytes: # 0
   <= 2432 Bytes: # 0
   <= 2560 Bytes: # 0
   <= 2688 Bytes: # 0
   <= 2816 Bytes: # 0
   <= 2944 Bytes: # 0
   <= 3072 Bytes: # 0
   <= 3200 Bytes: # 0
   <= 3328 Bytes: # 0
   <= 3456 Bytes: # 0
   <= 3584 Bytes: # 0
   <= 3712 Bytes: # 0
   <= 3840 Bytes: # 0
   <= 3968 Bytes: # 0
   <= 4096 Bytes: # 0

Estimated Compression Ratio with ScaleFlux: 3.0:1
```

## Sample Output (Directory Scan)
```
$ sudo ./c_est -d /mnt -t 24
Processing /mnt as a directory using 24 threads
294.9 GiB Completed (101 files) with 22 threads active

Total Bytes Analyzed     : 317399691264
Total Files Analyzed     : 101
All Zero (Empty) Sectors : 0
Incompressible Sectors   : 0

Compressibility Histogram:

   <=  128 Bytes: ## 3755793
   <=  256 Bytes: # 7989
   <=  384 Bytes: # 7271
   <=  512 Bytes: # 8948
   <=  640 Bytes: # 12610
   <=  768 Bytes: # 13679
   <=  896 Bytes: # 31025
   <= 1024 Bytes: # 81863
   <= 1152 Bytes: # 329334
   <= 1280 Bytes: # 840626
   <= 1408 Bytes: # 2649259
   <= 1536 Bytes: ################################################## 69724945
   <= 1664 Bytes: # 26817
   <= 1792 Bytes: # 0
   <= 1920 Bytes: # 0
   <= 2048 Bytes: # 0
   <= 2176 Bytes: # 0
   <= 2304 Bytes: # 0
   <= 2432 Bytes: # 0
   <= 2560 Bytes: # 0
   <= 2688 Bytes: # 0
   <= 2816 Bytes: # 0
   <= 2944 Bytes: # 0
   <= 3072 Bytes: # 0
   <= 3200 Bytes: # 0
   <= 3328 Bytes: # 0
   <= 3456 Bytes: # 0
   <= 3584 Bytes: # 0
   <= 3712 Bytes: # 0
   <= 3840 Bytes: # 0
   <= 3968 Bytes: # 0
   <= 4096 Bytes: # 0

Estimated Compression Ratio with ScaleFlux: 3.0:1
```
