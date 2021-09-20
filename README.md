# RDBMS
A simple rdbm with a paged file manager, record based file manager, a relation manager, a index manager, and a query engine.
Built in C++, and runs on linux.

## Paged File Manager:
The paged file manager(PFM) is the most simple of the components. 
It opens files and returns a filehandler, and also closes files. Second, 

### FileHandler
A object exposed by the PFM that enforces the usage of reading and writing to an opened files via 4098 Byte buffers or 'pages'.

## Record Based File Manager:
The record based file manager(RBFM), defines the logic of how the information on each page is organized. 
Each page is fully comprised by three distinct sections, the record heap, empty space, and a directory. 
The Record heap always begins at the begining of a page, while the empty space always takes up the middle of the page, and the directory takes up the end of the page. 
To enforce the organization of record based files, the RBFM implements the logic for inserting, updating, deleting, and reading records.

### Record Heap:
It each record is divided into two ordered parts, null indicators followed by the list of attributes. 
Attributes can take four types, NULL, INT, FLOAT, or VARCHAR. If the null indicators are a single bit in size, and mark whether the attribute is NULL. 
Both integers and floats are stored with 4 bytes, while VARCHARS are stored with 4 bytes to indicate their length, followed by the indicated number of bytes.

### Page Directory:
To keep track of the records on the page, the directory keeps track of how many total records are on the page, the total number of bytes being used by the record section, and a 8 byte directory entry of each record. Each directory entry keeps track of the byte offset of where the record starts, and the total byte size of the record. The directory entries are kept in the same order as the records themselves.

### RID (Record ID):
A RID is a tuple of the page the record is stored on and the index number of the record in the ordered list of records.
Once a record is inserted into a file, its RID is gauranteed to never change. In order to accomplish this, special care needs to be taken when records are updated or deleted.
Deleted records have their information on the record heap completely removed, but keep their directory entry. 
Updated records that become to large to fit on the page, have their record heap data deleted, but their directory entry is turned into a pointer to another RID where the updated record is now stored.
So although an RID is gauranteed to be persistent, multiple RIDs may point to the same data.

## Relation Manager:
The relation manager organizes the relations by storing them as files, and also keeps track of the attributes of each relation. To do this it creates three special system files, the tables file, the columns file and the index file.

## Index Manager:
The index manager creates indexes using B+ trees.

## Query Engine:
