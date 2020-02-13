# hogletdb
A real description comes later. At the moment, hoglet is planned to be a plain java embedded key value store based on ideas like lsm and wisckey for millions of key/value pairs optimized for storage on HDD and SSD devices.
At the moment it's more a concept implementation than a productive embeddable key/value store. But as the implementing feature list grows, the first release for productive use will be within reach. 
There are two very different use cases, where this store is designt for.
First implementing a fast blob storage for CDN usage. The entries will be splitted into chunks and loaded from the storage as a stream of data. Access will always be given with a simple key, updates to the values are rare. Overall value size is about > 20TB of data. 

The second design goal is a key/value store used for thousend of millions of small key/value pairs with high update frequency and time to life definition (retention time)

#TODO

- Retention time for entries.
- auto compress vlog files (settings for e.g. every vlog must be compressed 

## Derzeitige Schwierigkeiten: ##

Besteht ein Zugriff (z.B. durch Cursors oder direkten Key Zugriff auf eine SST, kann diese während des Compact Prozesses nicht gelöscht werden. Das Problem ist besteht sogar weitgehender, denn selbst wenn die Dateien gelöscht werden können, kann es passieren, daß der Cursor dann auf den neuen SST der nächsten Ebene arbeitet und dann zu einem späteren Zeitpunkt wieder den gleichen Satz der Keys verarbeitet. Das muss verhinder werden.

Cursor Zeitpunkt A



> A1 A2 A3 A4 A5  
> B1 B2

A wird compacted, kann aber wegen dem Cursor nicht gelöscht werden

Compact

> A1 A2 A3 A4 A5  
> B1 B2 B3  

Cursor wander auf Ebene B weiter, A kann gelöscht werden.

> B1 B2 B3  

Wenn nun der Cursor zu B3 kommt, hat er den Inhalt bereits in seiner Ax verarbeitung verarbeitet.

Lösung: Konsequentes Snapshotting zu Beginn des Cursors. Für einen Cursor muss die Dateistruktur während seiner Ausführung stabil bleiben. 
D.h. Dateien dürfen nur gelöscht werden, wenn es keine aktive Cursorverbindung mehr gibt. D.h. alle Cursor müssen zumindest mit ihtm Verarbeitungszeiger nach der zu löschenden Datei stehen.

Weiterhin müssen die Ebenen ebenfalls in einem Snapshot zusammen gefasst werden. D.h. es wird pro Ebene eine neue Größe, reincarnation eingeführt. 
Wir eine Ebene compacted, wird eine neue Reincarnation angelegt. Alle neu zu erstellenden Dateien dieser Ebene werden dieser Reincarnation zu geordnet. Die Cursor Snapshots bleiben davon unberührt, da diese weiterhin auf der alten Reincarnation arbeiten. Eine Reincarnation kann nur gelöscht werden, wenn es keine Snapshot mehr gibt, der auf dieser arbeitet.

