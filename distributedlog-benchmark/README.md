#### How to run journal benchmark

- run ./scripts/snapshot to build a snapshot version of release packages
- copy the benchmark package to a test node : scp dist/release/distributedlog-benchmark-\*.zip <host>:~/
- go to the test node and unzip the package : unzip distributedlog-benchmark-\*.zip
- cd distributedlog-benchmark
- make sure you change conf/bookie.conf : point `journalDirectory` to the disk you want to test
- tune any JVM settings accordingly
- run the test with : bin/dbench com.twitter.distributedlog.benchmark.Benchmarker -bc conf/bookie.conf --rate 40000 --change-rate 10000 --max-rate 400000 --change-interval 600 --concurrency 1 -ms 1024 -p org.apache.bookkeeper.stats.CodahaleMetricsServletProvider -d 3600 -i 0 -m journal
