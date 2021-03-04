# index-kv

The machine has a 1T unordered data file of the form `(key_size, key, value_size, value)`, where all keys are not the same.

Design an index structure such that concurrent random reads of each key-value are minimally expensive; the key must be in the file at the time of read and roughly conform to the Zipf's distribution.
Allow preprocessing of the data file, but the preprocessing time is factored into the cost of the entire process.

## Spec

* CPU: 8 cores
* MEM: 4G
* HDD: 4T
