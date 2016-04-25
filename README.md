# hzdfs
This is a file distribution solution over Hazelcast.

The distributed file system is modelled as a distributed map of file records. Each record is a simply a UTF8 text of each line of the file. The data distribution is, however, optimized by memory mapped reading of byte chunks instead of a plain BufferedReader#readLine() type iteration.

Then Hazelcast APIs (including mapreduce) can be invoked on the resultant map for analyzing.
