# hogletdb
A real description comes later. At the moment, hoglet is planned to be a plain java embedded key value store based on ideas like lsm and wisckey for millions of key/value pairs optimized for storage on HDD and SSD devices.
At the moment it's more a concept implementation than a productive embeddable key/value store. But as the implementing feature list grows, the first release for productive use will be within reach. 
There are two very different use cases, where this store is designt for.
First implementing a fast blob storage for CDN usage. The entries will be splitted into chunks and loaded from the storage as a stream of data. Access will always be given with a simple key, updates to the values are rare. Overall value size is about > 20TB of data. 

The second design goal is a key/value store used for thousend of millions of small key/value pairs with high update frequency and time to life definition (retention time)

#TODO

- Retention time for entries.
- auto compress vlog files (settings for e.g. every vlog must be compressed 
