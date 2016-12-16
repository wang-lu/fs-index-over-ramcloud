#ifndef TABLE_FS_H
#define TABLE_FS_H

#define FUSE_USE_VERSION 26
#include <fuse.h>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <unordered_map>
#include <errno.h>
#include "fs/tfs_inode.h"
#include "util/properties.h"
#include "util/logging.h"
#include "ramcloud/RamCloud.h"

namespace TestFS {
const char idtable[] = "idtable";
const char metatable[] = "metatable";
enum InodeAccessMode {
	INODE_READ = 0, INODE_DELETE = 1, INODE_WRITE = 2,
};

enum InodeState {
	CLEAN = 0, DELETED = 1, DIRTY = 2,
};

class TestFS {
public:
	~TestFS() {
	}


	void* Init(struct fuse_conn_info *conn);

	void Destroy(void * data);

	int GetAttr(const char *path, struct stat *statbuf);

	int Open(const char *path, struct fuse_file_info *fi);

	int Read(const char* path, char *buf, size_t size, off_t offset,
			struct fuse_file_info *fi);

	int Write(const char* path, const char *buf, size_t size, off_t offset,
			struct fuse_file_info *fi);

	int Truncate(const char *path, off_t offset);

	int Fsync(const char *path, int datasync, struct fuse_file_info *fi);

	int Release(const char *path, struct fuse_file_info *fi);

	int Readlink(const char *path, char *buf, size_t size);

	int Symlink(const char *target, const char *path);

	int Unlink(const char *path);

	int MakeNode(const char *path, mode_t mode, dev_t dev);

	int MakeDir(const char *path, mode_t mode);

	int OpenDir(const char *path, struct fuse_file_info *fi);

	int ReadDir(const char *path, void *buf, fuse_fill_dir_t filler,
			off_t offset, struct fuse_file_info *fi);

	int ReleaseDir(const char *path, struct fuse_file_info *fi);

	int RemoveDir(const char *path);

	int Rename(const char *new_path, const char *old_path);

	int Access(const char *path, int mask);

	int UpdateTimens(const char *path, const struct timespec tv[2]);

	int Chmod(const char *path, mode_t mode);

	int Chown(const char *path, uid_t uid, gid_t gid);


private:
	std::string datadir;
        std::string mountdir;
        tfs_inode_t max_inode_num;
        RAMCloud::RamCloud* cluster;
        Logging* logs;
	bool flag_fuse_enabled;
	uint64_t idt;
	uint64_t mdt;
	uint64_t threshold;
	
	int Setup(Properties& prop);
	bool IsEmpty() {
                return (max_inode_num == 0);
        }
        tfs_inode_t NewInode();

	inline int FSError(const char *error_message);
	
	inline void DeleteDBFile(tfs_inode_t inode_id, int filesize);

	inline void GetDiskFilePath(char *path, tfs_inode_t inode_id);

	inline int OpenDiskFile(const tfs_inode_header* iheader, int flags);

	inline int TruncateDiskFile(tfs_inode_t inode_id, off_t new_size);

	inline ssize_t MigrateDiskFileToBuffer(tfs_inode_t inode_it, char* buffer,
			size_t size);

	int MigrateToDiskFile(RAMCloud::Buffer* rcbuf, int &fd, int flags);

	inline void CloseDiskFile(int& fd_);

	inline void InitStat(struct stat &statbuf, tfs_inode_t inode, mode_t mode,
			dev_t dev);

	tfs_inode_val_t InitInodeValue(tfs_inode_t inum, mode_t mode, dev_t dev,
			std::string filename);

	std::string InitInodeValue(const std::string& old_value,
			std::string filename);

	void FreeInodeValue(tfs_inode_val_t &ival);

	bool ParentPathLookup(const char* path,  RAMCloud::KeyInfo *mykeylist,tfs_inode_t &inode_in_search, const char* &lastdelimiter);

	inline bool PathLookup(const char *path, RAMCloud::KeyInfo *mykeylist,std::string &filename);

	inline bool PathLookup(const char *path, RAMCloud::KeyInfo *mykeylist);

};

}
#endif
