# index-kv

The machine has a 1T unordered data file of the form `(key_size, key, value_size, value)`, where all keys are not the same.

Design an index structure such that concurrent random reads of each key-value are minimally expensive; the key must be in the file at the time of read and roughly conform to the Zipf's distribution.

Allow preprocessing of the data file, but the preprocessing time is factored into the cost of the entire process.

## Spec

* CPU: 8 cores
* MEM: 4G
* HDD: 4T

## Solution

* **Zipf's Law**: Zipf's law refers to the fact that for many types of data studied in the physical and social sciences, the rank-frequency distribution is an inverse relation. The law indicates that the test data is highly data localized, i.e., a small amount of data constitutes the majority of the test cases. In this repo I used golang's `rand.Zipf` to complete the following experiments.
  
* **LRU Cache**: Because the query data is localized, it is obvious that the query should be accelerated using LRU cache.
* **Hash & Sharding**  `(hash, offset)`
  
  Considering that there is a large amount of data on the hard disk, we can't read it all into the memory. So we need to hash all the keys and store them in different shards. Given possible hash collisions, I designed to store the location of each key in one shard corresponding to its position in the original data. When querying, all the positions of the current hash are read and compared one by one in the original data until the key matches.
  
  The following explains my two methods of organizing data chunks:
  
  * **Splay**: A splay tree is a binary search tree with the additional property that recently accessed elements are quick to access again. Good performance for a splay tree depends on the fact that it is self-optimizing, in that frequently accessed nodes will move nearer to the root where they can be accessed more quickly. 
  * **HashMap**: builtin `map` in Golang, simple but effective

## UT

* **cache/cache_test.go**: Unit test for LRU cache
* **chunk/chunk_test.go**: Unit test for chunk file
* **spaly/splay_test.go**: Unit test for splay data structure
* **index/index_test.go**: Unit test and benchmark for index interface

## Benchmark

The following data were tested under the use of 1e6 randomly generated KV pairs(1GB) and may show some bias.

> goos: darwin
> goarch: amd64

* benchmark for one query with different index options

|Test Flag|Time Per Query (s)|Bytes Processed Per Query (B)|Allocations Per Query|
|:---:|:---:|:---:|:---:|
|LRU-Splay|9.4858e-5|1462|7|
|LRU-HashMap|1.09869e-4|1679|7|
|Splay|1.79937e-4|2955|27|
|HashMap|2.08686e-4|3998|30|

* benchmark for fetching one item with use builtin `map` and splay

|Test Flag|Time Per Query (s)|Bytes Processed Per Query (B)|Allocations Per Query|
|:---:|:---:|:---:|:---:|
|splay|4.838e-6|48|1|
|hashmap|5.59e-8|0|0|

## Code Coverage

|Module|Coverage|
|---|---|
|cache|85.7%|
|chunk|68.3%|
|index|77.5%|
|splay|97.1%|
