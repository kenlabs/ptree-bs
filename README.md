



## Benchmark Test

Win11  I5-12600KF

**Note**: Cache size is the max number of items in lru cache instead of max bytes length in cache. Tree Size is the number of k/v pairs built from

**Builder Tree**

| Tree Size | Cache Size | Cost Time  |
| --------- | ---------- | ---------- |
| 1000      | 0          | 28.6169ms  |
| 10000     | 0          | 248.2452ms |
| 100000    | 1 << 20    | 2.813035s  |
| 100000    | 0          | 2.3904409s |



**Read Test**

Based on a 500K items static tree

| Random Read Times | Cache Size | Chunk Strategy | Cost Time   |
| ----------------- | ---------- | -------------- | ----------- |
| 50000             | 0          | KeySplitter    | 24.9104392s |
| 50000             | 0          | RollingHash    | 16.4367341s |
| 50000             | 1<<14      | KeySplitter    | 79.3775ms   |
| 50000             | 1<<14      | RollingHash    | 79.7494ms   |
| 50000             | 1<<12      | KeySplitter    | 9.2103152s  |
| 50000             | 1<<12      | RollingHash    | 6.5867881s  |



**Write Test**

Note: random insert and  batch insert.

| Tree Size | Cache Size | Insert Items Number | Chunk Strategy | CostTime   |
| --------- | ---------- | ------------------- | -------------- | ---------- |
| 1000      | 1 <<15     | 500                 | KeySplitter    | 64.24ms    |
| 1000      | 0          | 500                 | KeySplitter    | 68.6848ms  |
| 10000     | 1 << 18    | 5000                | KeySplitter    | 624.0123ms |
| 10000     | 0          | 5000                | KeySplitter    | 631.2595ms |
| 100000    | 1 << 24    | 50000               | KeySplitter    | 6.1018462s |
| 100000    | 0          | 50000               | KeySplitter    | 6.4196689s |
| 1000      | 1 <<15     | 500                 | RollingHash    | 51.222ms   |
| 1000      | 0          | 500                 | RollingHash    | 59.7988ms  |
| 10000     | 1 << 18    | 5000                | RollingHash    | 643.372ms  |
| 10000     | 0          | 5000                | RollingHash    | 538.2212ms |
| 100000    | 1 << 24    | 50000               | RollingHash    | 5.3276151s |
| 100000    | 0          | 50000               | RollingHash    | 5.6442758s |
|           |            |                     |                |            |

