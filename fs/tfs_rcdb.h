
#ifndef TFS_RCDB_H_
#define TFS_RCDB_H_

#include <sys/stat.h>
#include <stdint.h>
#include "IndexLookup.h"
#include "IndexKey.h"
#include "tfs_inode.h"
#include "RamCloud.h"


namespace TestFS {
	uint64_t ConnectDB(RAMCloud::RamCloud *cluster,char *tablename);
	uint64_t GetNextID(RAMCloud::RamCloud *cluster,uint64_t tableid);
	uint64_t GetCurrentID(RAMCloud::RamCloud *cluster,uint64_t tableid);
	int MakeMetaKey(char* filename, tfs_inode_t parentid,RAMCloud::KeyInfo *mykeylist);
	int GetRamCloudBuffer(RAMCloud::RamCloud *cluster,RAMCloud::KeyInfo mykey,uint64_t tableid,RAMCloud::Buffer *value);
	std::string CopytoString(RAMCloud::RamCloud *cluster,RAMCloud::KeyInfo mykey, uint64_t tableid);
	int WriteString(RAMCloud::RamCloud *cluster, RAMCloud::KeyInfo *mykeylist,uint64_t tableid,std::string value);
	int WriteString(RAMCloud::RamCloud *cluster, RAMCloud::KeyInfo *mykeylist,uint64_t tableid,tfs_inode_val_t inode_val);
}

#endif
