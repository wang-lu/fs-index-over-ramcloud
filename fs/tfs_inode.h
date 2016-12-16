
#ifndef TFS_INODE_H_
#define TFS_INODE_H_

#include <sys/stat.h>
#include <stdint.h>
#include "IndexLookup.h"
#include "IndexKey.h"


namespace TestFS {

typedef uint64_t tfs_inode_t;
typedef uint64_t tfs_hash_t;
typedef struct stat tfs_stat_t;

static const char PATH_DELIMITER = '/';
static const int INODE_PADDING = 104;
static const int MAX_PATH_LEN = 256;
static const tfs_inode_t ROOT_INODE_ID = 0;
static const int NUM_FILES_IN_DATADIR_BITS = 14;
static const int NUM_FILES_IN_DATADIR = 16384;
static const int MAX_OPEN_FILES = 512;
static const char* ROOT_INODE_STAT = "/tmp/";


struct tfs_inode_header {
	tfs_stat_t fstat;
	char padding[INODE_PADDING];
	uint32_t has_blob;
	uint32_t namelen;
};

static const size_t TFS_INODE_HEADER_SIZE = sizeof(tfs_inode_header);
static const size_t TFS_INODE_ATTR_SIZE = sizeof(struct stat);

struct tfs_inode_val_t {
	size_t size;
	char* value;

	tfs_inode_val_t() :
			value(NULL), size(0) {
	}

	std::string ToString() const {
		return std::string(value, size);
	}
};

}

#endif
