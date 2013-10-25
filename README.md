hadoop_sandbox
==============

My sandbox of Hadoop jobs

1. **hdfs_file_scanner:** demonstrates how to programmatically (in java) scan through a set of files. While this uses Hadoop's local file operations, it may be easily shifted to use the HDFS file library.
2. **stemmedwordcount:** a simple derivation of the "hello world" wordcount MapReducer that employs Lucene's stemming
3. **customwriter:** demonstrates how to create your own writer for passing between MR jobs. To create a custom key, extend WritableComparable
