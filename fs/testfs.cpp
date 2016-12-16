#define FUSE_USE_VERSION 26
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cstdarg>
#include <error.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <sys/time.h>
#include <vector>
#include <algorithm>
#include <pthread.h>
#include <sstream>
#include "fs/testfs.h"
#include "fs/tfs_inode.h"
#include "fs/tfs_rcdb.h"
#include "util/myhash.h"
#include "util/socket.h"
#include "ramcloud/RamCloud.h"
#include "ramcloud/ClientException.h"

namespace TestFS {

int TestFS::Setup(Properties& prop) {
        char resolved_path[4096];
        char* ret;
        char ramcloud_endpoint[4096];
        ret = realpath(prop.getProperty("datadir").c_str(), resolved_path);
        datadir = std::string(resolved_path);
        ret = realpath(prop.getProperty("mountdir").c_str(), resolved_path);
        mountdir = std::string(resolved_path);
        ramcloud_endpoint = prop.getProperty("ramcloud_endpoint").c_str();

        if (access(datadir.c_str(), W_OK) > 0
                        || access(metadir.c_str(), W_OK) > 0) {
                fprintf(stderr, "cannot open directory!\n");
                exit(1);
        }

        logs = new Logging(prop.getProperty("logfile", ""));
        logs->SetDefault(logs);
        logs->Open();

        try {
                cluster = new RAMCloud::RamCloud(ramcloud_endpoint, "__unnamed__");
        } catch (RAMCloud::ClientException& e) {
                fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
                return 1;
        } catch (RAMCloud::Exception& e) {
                fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
                return 1;
        }

        logs->LogMsg("Connecting two databases.\n");
	try{
		idt=ConnectDB(&cluster,idtable);
	}catch(TableDoesntExistException){
		logs->LogMsg("Cannot find table %s at %s\n",idtable,ramcloud_endpoint);
		logs->LogMsg("Initiating a new one...\n");
		idt=cluster.createTable(idtable);
	}

	try{
		mdt=ConnectDB(&cluster,metatable);
	}catch(TableDoesntExistException){
		logs->LogMsg("Cannot find table %s at %s\n",metatable,ramcloud_endpoint);
		logs->LogMsg("Initiating a new one...\n");
		idt=cluster.createTable(metatable);
	}

        return 0;
}
void Destroy() {
        if (cluster != NULL) {
                delete cluster;
        }
        if (logs != NULL)
                delete logs;
}

tfs_inode_t NewInode() {
        max_inode_num=GetNextID(&cluster,idt);
        if (max_inode_num % (NUM_FILES_IN_DATADIR) == 0) {
                char fpath[512];
                sprintf(fpath, "%s/%d", datadir_.data(), (int) max_inode_num >> 14);
                mkdir(fpath, 0777);
        }
        return max_inode_num;
}


struct tfs_file_handle_t {
	int flags_;
	int fd_;
	InodeAccessMode mode_;
	KeyInfo *keylist_;   
	tfs_file_handle_t() :flags_(-1),fd_(-1),mode_(0),keylist_(NULL) {
	}
};


const tfs_inode_header *GetInodeHeader(const RAMCloud::Buffer &value) {
	return reinterpret_cast<const tfs_inode_header*>(value.getRange(0,value.size()));
}
const tfs_inode_header *GetInodeHeader(const std::string &value) {
	return reinterpret_cast<const tfs_inode_header*>(value.data());
}
const tfs_stat_t *GetAttribute(RAMCloud::Buffer &value) {
	return reinterpret_cast<const tfs_stat_t*>(value.getRange(0, value.size()));
}
const tfs_stat_t *GetAttribute(std::String &value) {
	return reinterpret_cast<const tfs_stat_t*>(value.data());
}

size_t GetInlineData(RAMCloud::Buffer &value, char* buf, size_t offset,
		size_t size) {
	const tfs_inode_header* header = GetInodeHeader(value);
	size_t realoffset = TFS_INODE_HEADER_SIZE + header->namelen + 1 + offset;
	if (realoffset < value.size()) {
		if (realoffset + size > value.size()) {
			size = value.size() - realoffset;
		}
		//use value.copy (uint32_t offset, uint32_t length, void *dest)
		value.copy(realoffset,size,buf);
		return size;
	} else {
		return 0;
	}
}
void UpdateIhandleValue(std::string &value, const char* buf, size_t offset,
		size_t size) {
	if (offset > value.size()) {
		value.resize(offset);
	}
	value.replace(offset, size, buf, size);
}

void UpdateInodeHeader(std::string &value, tfs_inode_header &new_header) {
	UpdateIhandleValue(value, (const char *) &new_header, 0,
			TFS_INODE_HEADER_SIZE);
}

void UpdateAttribute(std::string &value, const tfs_stat_t &new_fstat) {
	UpdateIhandleValue(value, (const char *) &new_fstat, 0,
			TFS_INODE_ATTR_SIZE);
}

void UpdateInlineData(std::string &value, const char* buf, size_t offset,size_t size) {
	const tfs_inode_header* header = GetInodeHeader(value);
	size_t realoffset = TFS_INODE_HEADER_SIZE + header->namelen + 1 + offset;
	UpdateIhandleValue(value, buf, realoffset, size);
}

void TruncateInlineData(std::string &value, size_t new_size) {
	const tfs_inode_header* header = GetInodeHeader(value);
	size_t target_size = TFS_INODE_HEADER_SIZE + header->namelen + new_size + 1;
	value.resize(target_size);
}

void DropInlineData(std::string &value) {
	const tfs_inode_header* header = GetInodeHeader(value);
	size_t target_size = TFS_INODE_HEADER_SIZE + header->namelen + 1;
	value.resize(target_size);
}


int TestFS::FSError(const char *err_msg) {
	int retv = -errno;
#ifdef TESTFS_DEBUG
	logs->LogMsg(err_msg);
#endif
	return retv;
}

void TestFS::InitStat(tfs_stat_t &statbuf, tfs_inode_t inode, mode_t mode,dev_t dev) {
	statbuf.st_ino = inode;
	statbuf.st_mode = mode;
	statbuf.st_dev = dev;

	if (flag_fuse_enabled) {
		statbuf.st_gid = fuse_get_context()->gid;
		statbuf.st_uid = fuse_get_context()->uid;
	} else {
		statbuf.st_gid = 0;
		statbuf.st_uid = 0;
	}

	statbuf.st_size = 0;
	statbuf.st_blksize = 0;
	statbuf.st_blocks = 0;
	if S_ISREG(mode) {
		statbuf.st_nlink = 1;
	} else {
		statbuf.st_nlink = 2;
	}
	time_t now = time(NULL);
	statbuf.st_atim.tv_nsec = 0;
	statbuf.st_mtim.tv_nsec = 0;
	statbuf.st_ctim.tv_sec = now;
	statbuf.st_ctim.tv_nsec = 0;
}

tfs_inode_val_t TestFS::InitInodeValue(tfs_inode_t inum, mode_t mode, dev_t dev,
		std::string filename) {
	tfs_inode_val_t ival;
	ival.size = TFS_INODE_HEADER_SIZE + filename.size() + 1;
	ival.value = new char[ival.size];
	tfs_inode_header* header = reinterpret_cast<tfs_inode_header*>(ival.value);
	InitStat(header->fstat, inum, mode, dev);
	header->has_blob = 0;
	header->namelen = filename.size();
	char* name_buffer = ival.value + TFS_INODE_HEADER_SIZE;
	memcpy(name_buffer, filename.data(), filename.size());
	name_buffer[header->namelen] = '\0';
	return ival;
}

std::string TestFS::InitInodeValue(const std::string& old_value,std::string filename) {
	std::string new_value = old_value;
	tfs_inode_header header = *GetInodeHeader(old_value);
	new_value.replace(TFS_INODE_HEADER_SIZE, header.namelen + 1,filename.data(), filename.size() + 1);
	header.namelen = filename.size(); 
	UpdateInodeHeader(new_value, header); //maybe do not need
	return new_value;
}

void TestFS::FreeInodeValue(tfs_inode_val_t &ival) {
	if (ival.value != NULL) {
		delete[] ival.value;
		ival.value = NULL;
	}
}

bool TestFS::ParentPathLookup(const char *path, RAMCloud::KeyInfo &mykeylist,
		tfs_inode_t &inode_in_search, const char* &lastdelimiter) {
	const char* lpos = path;
	const char* rpos;
	bool flag_found = true;
	std::string item;
	inode_in_search = ROOT_INODE_ID;
	while ((rpos = strchr(lpos + 1, PATH_DELIMITER)) != NULL) {
		if (rpos - lpos > 0) {
			MakeMetaKey(lpos + 1, rpos - lpos - 1, inode_in_search, &mykeylist);
			Buffer result;
			int ret=GetRamCloudBuffer(&cluster,mykeylist[0],metaid,&result);
			if (ret == 0) {
				inode_in_search = GetAttribute(result)->st_ino;
				result.reset(); 
			} else {
				errno = ENOENT;
				flag_found = false;
			}
			if (!flag_found) {
				return false;
			}
		}
	}
	lpos = rpos;
	if (lpos == path) {
		MakeMetaKey(NULL, 0, ROOT_INODE_ID, key);
	}
	lastdelimiter = lpos;
	return flag_found;
}

bool TestFS::PathLookup(const char *path, RAMCloud::KeyInfo &key) {
	const char* lpos;
	tfs_inode_t inode_in_search;
	if (ParentPathLookup(path, key, inode_in_search, lpos)) {
		const char* rpos = strchr(lpos, '\0');
		if (rpos != NULL && rpos - lpos > 1) {
			BuildMetaKey(lpos + 1, rpos - lpos - 1, inode_in_search, key);
		}
		return true;
	} else {
		errno = ENOENT;
		return false;
	}
}

bool TestFS::PathLookup(const char *path, RAMCloud::KeyInfo *mykeylist,std::string &filename) {
	const char* lpos;
	tfs_inode_t inode_in_search;
	if (ParentPathLookup(path, key, inode_in_search, lpos)) {
		const char* rpos = strchr(lpos, '\0');
		if (rpos != NULL && rpos - lpos > 1) {
			MakeMetaKey(lpos + 1, rpos - lpos - 1, inode_in_search, mykeylist);
			filename = std::string(lpos + 1, rpos - lpos - 1);
		} else {
			filename = std::string(lpos, 1);
		}
		return true;
	} else {
		errno = ENOENT;
		return false;
	}
}

void* TestFS::Init(struct fuse_conn_info *conn) {
	logs->LogMsg("TestFS initialized.\n");
	if (conn != NULL) {
		flag_fuse_enabled = true;
	} else {
		flag_fuse_enabled = false;
	}
	if (IsEmpty()) {
		logs->LogMsg("TestFS create root inode.\n");
		RAMCloud::KeyInfo mykeylist[2];
		BuildMetaKey(NULL, 0, ROOT_INODE_ID, mykeylist);
		struct stat statbuf;
		lstat(ROOT_INODE_STAT, &statbuf);
		tfs_inode_val_t value = InitInodeValue(ROOT_INODE_ID, statbuf.st_mode,
				statbuf.st_dev, std::string("\0"));
		try {
			WriteString(&cluster,mykeylist,mdt,value);
		} catch (RamCloudClientException e) {
			logs->LogMsg("TestFS create root directory failed.\n");
		}
		FreeInodeValue(value);
	}
}

void TestFS::Destroy(void * data) {
	logs->LogMsg("file system unmounted.\n");
}

int TestFS::GetAttr(const char *path, struct stat *statbuf) {
	RAMCloud::KeyInfo mykeylist[2];
	if (!PathLookup(path, mykeylist)) {
		return FSError("GetAttr Path Lookup: No such file or directory: %s\n");
	}
	int ret = 0;
	ramcloud::Buffer rcbuf;

size_t GetInlineData(RAMCloud::Buffer &value, char* buf, size_t offset,
		size_t size) {
	const tfs_inode_header* header = GetInodeHeader(value);
	size_t realoffset = TFS_INODE_HEADER_SIZE + header->namelen + 1 + offset;
	if (realoffset < value.size()) {
		if (realoffset + size > value.size()) {
			size = value.size() - realoffset;
		}
		//use value.copy (uint32_t offset, uint32_t length, void *dest)
		value.copy(realoffset,size,buf);
		return size;
	} else {
		return 0;
	}
}
void UpdateIhandleValue(std::string &value, const char* buf, size_t offset,
		size_t size) {
	if (offset > value.size()) {
		value.resize(offset);
	}
	value.replace(offset, size, buf, size);
}

void UpdateInodeHeader(std::string &value, tfs_inode_header &new_header) {
	UpdateIhandleValue(value, (const char *) &new_header, 0,
			TFS_INODE_HEADER_SIZE);
}

void UpdateAttribute(std::string &value, const tfs_stat_t &new_fstat) {
	UpdateIhandleValue(value, (const char *) &new_fstat, 0,
			TFS_INODE_ATTR_SIZE);
}

void UpdateInlineData(std::string &value, const char* buf, size_t offset,size_t size) {
	const tfs_inode_header* header = GetInodeHeader(value);
	size_t realoffset = TFS_INODE_HEADER_SIZE + header->namelen + 1 + offset;
	UpdateIhandleValue(value, buf, realoffset, size);
}

void TruncateInlineData(std::string &value, size_t new_size) {
	const tfs_inode_header* header = GetInodeHeader(value);
	size_t target_size = TFS_INODE_HEADER_SIZE + header->namelen + new_size + 1;
	value.resize(target_size);
}

void DropInlineData(std::string &value) {
	const tfs_inode_header* header = GetInodeHeader(value);
	size_t target_size = TFS_INODE_HEADER_SIZE + header->namelen + 1;
	value.resize(target_size);
}


int TestFS::FSError(const char *err_msg) {
	int retv = -errno;
#ifdef TESTFS_DEBUG
	logs->LogMsg(err_msg);
#endif
	return retv;
}

void TestFS::InitStat(tfs_stat_t &statbuf, tfs_inode_t inode, mode_t mode,dev_t dev) {
	statbuf.st_ino = inode;
	statbuf.st_mode = mode;
	statbuf.st_dev = dev;

	if (flag_fuse_enabled) {
		statbuf.st_gid = fuse_get_context()->gid;
		statbuf.st_uid = fuse_get_context()->uid;
	} else {
		statbuf.st_gid = 0;
		statbuf.st_uid = 0;
	}

	statbuf.st_size = 0;
	statbuf.st_blksize = 0;
	statbuf.st_blocks = 0;
	if S_ISREG(mode) {
		statbuf.st_nlink = 1;
	} else {
		statbuf.st_nlink = 2;
	}
	time_t now = time(NULL);
	statbuf.st_atim.tv_nsec = 0;
	statbuf.st_mtim.tv_nsec = 0;
	statbuf.st_ctim.tv_sec = now;
	statbuf.st_ctim.tv_nsec = 0;
}

tfs_inode_val_t TestFS::InitInodeValue(tfs_inode_t inum, mode_t mode, dev_t dev,
		std::string filename) {
	tfs_inode_val_t ival;
	ival.size = TFS_INODE_HEADER_SIZE + filename.size() + 1;
	ival.value = new char[ival.size];
	tfs_inode_header* header = reinterpret_cast<tfs_inode_header*>(ival.value);
	InitStat(header->fstat, inum, mode, dev);
	header->has_blob = 0;
	header->namelen = filename.size();
	char* name_buffer = ival.value + TFS_INODE_HEADER_SIZE;
	memcpy(name_buffer, filename.data(), filename.size());
	name_buffer[header->namelen] = '\0';
	return ival;
}

std::string TestFS::InitInodeValue(const std::string& old_value,std::string filename) {
	std::string new_value = old_value;
	tfs_inode_header header = *GetInodeHeader(old_value);
	new_value.replace(TFS_INODE_HEADER_SIZE, header.namelen + 1,filename.data(), filename.size() + 1);
	header.namelen = filename.size(); 
	UpdateInodeHeader(new_value, header); //maybe do not need
	return new_value;
}

void TestFS::FreeInodeValue(tfs_inode_val_t &ival) {
	if (ival.value != NULL) {
		delete[] ival.value;
		ival.value = NULL;
	}
}

bool TestFS::ParentPathLookup(const char *path, RAMCloud::KeyInfo &mykeylist,
		tfs_inode_t &inode_in_search, const char* &lastdelimiter) {
	const char* lpos = path;
	const char* rpos;
	bool flag_found = true;
	std::string item;
	inode_in_search = ROOT_INODE_ID;
	while ((rpos = strchr(lpos + 1, PATH_DELIMITER)) != NULL) {
		if (rpos - lpos > 0) {
			MakeMetaKey(lpos + 1, rpos - lpos - 1, inode_in_search, &mykeylist);
			Buffer result;
			int ret=GetRamCloudBuffer(&cluster,mykeylist[0],metaid,&result);
			if (ret == 0) {
				inode_in_search = GetAttribute(result)->st_ino;
				result.reset(); 
			} else {
				errno = ENOENT;
				flag_found = false;
			}
			if (!flag_found) {
				return false;
			}
		}
	}
	lpos = rpos;
	if (lpos == path) {
		MakeMetaKey(NULL, 0, ROOT_INODE_ID, key);
	}
	lastdelimiter = lpos;
	return flag_found;
}

bool TestFS::PathLookup(const char *path, RAMCloud::KeyInfo &key) {
	const char* lpos;
	tfs_inode_t inode_in_search;
	if (ParentPathLookup(path, key, inode_in_search, lpos)) {
		const char* rpos = strchr(lpos, '\0');
		if (rpos != NULL && rpos - lpos > 1) {
			BuildMetaKey(lpos + 1, rpos - lpos - 1, inode_in_search, key);
		}
		return true;
	} else {
		errno = ENOENT;
		return false;
	}
}

bool TestFS::PathLookup(const char *path, RAMCloud::KeyInfo *mykeylist,std::string &filename) {
	const char* lpos;
	tfs_inode_t inode_in_search;
	if (ParentPathLookup(path, key, inode_in_search, lpos)) {
		const char* rpos = strchr(lpos, '\0');
		if (rpos != NULL && rpos - lpos > 1) {
			MakeMetaKey(lpos + 1, rpos - lpos - 1, inode_in_search, mykeylist);
			filename = std::string(lpos + 1, rpos - lpos - 1);
		} else {
			filename = std::string(lpos, 1);
		}
		return true;
	} else {
		errno = ENOENT;
		return false;
	}
}

void* TestFS::Init(struct fuse_conn_info *conn) {
	logs->LogMsg("TestFS initialized.\n");
	if (conn != NULL) {
		flag_fuse_enabled = true;
	} else {
		flag_fuse_enabled = false;
	}
	if (IsEmpty()) {
		logs->LogMsg("TestFS create root inode.\n");
		RAMCloud::KeyInfo mykeylist[2];
		BuildMetaKey(NULL, 0, ROOT_INODE_ID, mykeylist);
		struct stat statbuf;
		lstat(ROOT_INODE_STAT, &statbuf);
		tfs_inode_val_t value = InitInodeValue(ROOT_INODE_ID, statbuf.st_mode,
				statbuf.st_dev, std::string("\0"));
		try {
			WriteString(&cluster,mykeylist,mdt,value);
		} catch (RamCloudClientException e) {
			logs->LogMsg("TestFS create root directory failed.\n");
		}
		FreeInodeValue(value);
	}
}

void TestFS::Destroy(void * data) {
	logs->LogMsg("file system unmounted.\n");
}

int TestFS::GetAttr(const char *path, struct stat *statbuf) {
	RAMCloud::KeyInfo mykeylist[2];
	if (!PathLookup(path, mykeylist)) {
		return FSError("GetAttr Path Lookup: No such file or directory: %s\n");
	}
	int ret = 0;
	ramcloud::Buffer rcbuf;
	GetRamCloudBuffer(&cluster,mykeylist[0],mdt,&rcbuf);
	*statbuf = *(GetAttribute(rcbuf));
#ifdef TABLEFS_DEBUG
	logs->LogMsg("GetAttr DBKey: %s\n", mykeylist[0].key);
	logs->LogStat(path, statbuf);
#endif
	return ret;
}

void TestFS::GetDiskFilePath(char *path, tfs_inode_t inode_id) {
	sprintf(path, "%s/%d/%d", datadir.data(),
			(int) inode_id >> NUM_FILES_IN_DATADIR_BITS,
			(int) inode_id % (NUM_FILES_IN_DATADIR));
}

int TestFS::OpenDiskFile(const tfs_inode_header* iheader, int flags) {
	char fpath[128];
	GetDiskFilePath(fpath, iheader->fstat.st_ino);
	int fd = open(fpath, flags | O_CREAT, iheader->fstat.st_mode);
#ifdef  TABLEFS_DEBUG
	logs->LogMsg("OpenDiskFile: %s InodeID: %d FD: %d\n",
			fpath, iheader->fstat.st_ino, fd);
#endif
	return fd;
}

int TestFS::TruncateDiskFile(tfs_inode_t inode_id, off_t new_size) {
	char fpath[128];
	GetDiskFilePath(fpath, inode_id);
#ifdef  TABLEFS_DEBUG
	logs->LogMsg("TruncateDiskFile: %s, InodeID: %d, NewSize: %d\n",
			fpath, inode_id, new_size);
#endif
	return truncate(fpath, new_size);
}

size_t TestFS::MigrateDiskFileToBuffer(tfs_inode_t inode_id, char* buffer,
		size_t size) {
	char fpath[128];
	GetDiskFilePath(fpath, inode_id);
	int fd = open(fpath, O_RDONLY);
	ssize_t ret = pread(fd, buffer, size, 0);
	close(fd);
	unlink(fpath);
	return ret;
}

// need to change parameter in write usd std::string as input
int TestFS::MigrateToDiskFile(std::string &stringbuf, int &fd, int flags) {
	const tfs_inode_header* iheader = GetInodeHeader(stringbuf);
	if (fd >= 0) {
		close(fd);
	}
	fd = OpenDiskFile(iheader, flags);
	if (fd < 0) {
		fd = -1;
		return -errno;
	}
	int ret = 0;
	if (iheader->fstat.st_size > 0) {
		const char* buffer = (const char *) iheader
				+ (TFS_INODE_HEADER_SIZE + iheader->namelen + 1);
		if (pwrite(fd, buffer, iheader->fstat.st_size, 0)
				!= iheader->fstat.st_size) {
			ret = -errno;
		}
		DropInlineData(stringbuf);
	}
	return ret;
}

void TestFS::CloseDiskFile(int& fd_) {
	close(fd_);
	fd_ = -1;
}

int TestFS::Open(const char *path, struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
	logs->LogMsg("Open: %s, Flags: %d\n", path, fi->flags);
#endif

	RAMCloud::KeyInfo mykeylist[2];
	if (!PathLookup(path, mykeylist) {
		return FSError("Open: No such file or directory\n");
	}
	int ret = 0;
	tfs_file_handle_t* fh = new tfs_file_handle_t();
	fh->keylist_ = mykeylist;
	fh->flags_ = fi->flags;
	RAMCloud::Buffer rcbuf;
	GetRamCloudBuffer(&cluster,mykeylist,mdt,&rcbuf);
	const tfs_inode_header *iheader = GetInodeHeader(rcbuf);
	if (iheader->has_blob > 0) {
		fh->flag = fi->flags;
		fh->fd_ = OpenDiskFile(iheader, fh->flags_);
		if (fh->fd_ < 0) {
			ret = -errno;
		}
	}
#ifdef  TABLEFS_DEBUG
	logs->LogMsg("Open: %s,FD: %d\n",
			path,fh->fd_);
#endif
	if (ret == 0) {
		fi->fh = (uint64_t) fh;
	} else {
		delete fh;
	}
	return ret;
}

// need to readin data again, maybe save inode header pointer in fuse_file_info at open 
int TestFS::Read(const char* path, char *buf, size_t size, off_t offset,struct fuse_file_info *fi) {

#ifdef  TABLEFS_DEBUG
logs->LogMsg("Read: %s\n", path);
#endif

tfs_file_handle_t* fh = reinterpret_cast<tfs_file_handle_t*>(fi->fh);
RAMCloud::KeyInfo *mykeylist=fh->keylist_; 
ramcloud::Buffer rcbuf;
GetRamCloudBuffer(&cluster,*mykeylist,mdt,rcbuf);
const tfs_inode_header* iheader = GetInodeHeader(rcbuf);
int ret;
if (iheader->has_blob > 0) {
	if (fh->fd_ < 0) {
		fh->fd_ = OpenDiskFile(iheader, fh->flags_);
		if (fh->fd_ < 0)
			ret = -EBADF;
	}
	if (fh->fd_ >= 0) {
		ret = pread(fh->fd_, buf, size, offset);
	}
} else {

	ret = GetInlineData(rcbuf, buf, offset, size);
}
return ret;
}

int TestFS::Write(const char* path, const char *buf, size_t size, off_t offset,
	struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("Write: %s %lld %d\n", path, offset, size);
#endif

tfs_file_handle_t* fh = reinterpret_cast<tfs_file_handle_t*>(fi->fh);
RAMCloud::KeyInfo *mykeylist=fh->keylist_;
std::String strbuf=CopytoString(&cluster,*mykeylist,mdt);
const tfs_inode_header* iheader = GetInodeHeader(strbuf);
int ret = 0, has_imgrated = 0;
int has_larger_size = (iheader->fstat.st_size < offset + size) ? 1 : 0;

#ifdef  TABLEFS_DEBUG
logs->LogMsg("Write: %s has_larger_size %d old: %d new: %lld\n",
		path, has_larger_size, iheader->fstat.st_size, offset + size);
#endif

if (iheader->has_blob > 0) {
	if (fh->fd_ < 0) {
		fh->fd_ = OpenDiskFile(iheader, fh->flags_);
		if (fh->fd_ < 0)
			ret = -EBADF;
	}
	if (fh->fd_ >= 0) {
		ret = pwrite(fh->fd_, buf, size, offset);
	}
} else {                     //Today's mark
	if (offset + size > threshold) {
		size_t cursize = iheader->fstat.st_size;
		ret = MigrateToDiskFile(rcbuf, fh->fd_, fi->flags);
		if (ret == 0) {
			tfs_inode_header new_iheader = *GetInodeHeader(rcbuf);
			new_iheader.fstat.st_size = offset + size;
			new_iheader.has_blob = 1;
			UpdateInodeHeader(strbuf, new_iheader);
			has_imgrated = 1;
			ret = pwrite(fh->fd_, buf, size, offset);
		}
	} else {
		UpdateInlineData(strbuf, buf, offset, size);
		ret = size;
	}
}
if (ret >= 0) {
	if (has_larger_size > 0 && has_imgrated == 0) {
		tfs_inode_header new_iheader = *GetInodeHeader(strbuf);
		new_iheader.fstat.st_size = offset + size;
		UpdateInodeHeader(strbuf, new_iheader);
	}
}

#ifdef  TABLEFS_DEBUG
logs->LogMsg("Write: %s",path);
#endif
WriteString(&cluster,mykeylist,mdt,strbuf);
return ret;
}


// GetInodeHeader directly change metakey to iheader?
int TestFS::Fsync(const char *path, int datasync, struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("Fsync: %s\n", path);
#endif
tfs_file_handle_t* fh = reinterpret_cast<tfs_file_handle_t*>(fi->fh);
RAMCloud *mykeylist = fh->keylist_;
ramcloud::Buffer rcbuf;
GetRamCloudBuffer(&cluster,mykeylist,mdt,&rcbuf);
const tfs_inode_header* iheader = GetInodeHeader(rcbuf);
int ret = 0;
if (handle->mode_ == INODE_WRITE) {
	if (iheader->has_blob > 0) {
		ret = fsync(fh->fd_);
	}
	if (datasync == 0) {
		//ret = metadb->Sync();
	}
}

//fstree_lock.Unlock(handle->key_);
return -ret;
}

int TestFS::Release(const char *path, struct fuse_file_info *fi) {
tfs_file_handle_t* fh = reinterpret_cast<tfs_file_handle_t*>(fi->fh);
RAMCloud mykeylist = fh->keylist_;
ramcloud::Buffer rcbuf;
std::string myresult=CopyToString(&cluster,mykeylist,mdt);
if (fh->mode_ == INODE_WRITE) {
	const tfs_stat_t *value = GetAttribute(myresult);
	tfs_stat_t new_value = *value;
	new_value.st_atim.tv_sec = time(NULL);
	new_value.st_atim.tv_nsec = 0;
	new_value.st_mtim.tv_sec = time(NULL);
	new_value.st_mtim.tv_nsec = 0;
	UpdateAttribute(myresult, new_value);
}

#ifdef  TABLEFS_DEBUG
logs->LogMsg("Release: %s, FD: %d\n",
		path, fh->fd_);
#endif

int ret = 0;
if (fh->fd_ != -1) {
	ret = close(fh->fd_);
}

WriteString(&cluster,mykeylist,mdt,myresult);

if (ret != 0) {
	return -errno;
} else {
	return 0;
}
}

int TestFS::Truncate(const char *path, off_t new_size) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("Truncate: %s\n", path);
#endif

RAMCloud::KeyInfo mykeylist[2];
if (!PathLookup(path, mykeylist)) {
	return FSError("Open: No such file or directory\n");
}

int ret = 0;
std::string myresult=CopyToString(&cluster,mykeylist,mdt);
const tfs_inode_header *iheader = GetInodeHeader(myresult);
if (iheader->has_blob > 0) {
	if (new_size > threshold) {
		TruncateDiskFile(iheader->fstat.st_ino, new_size);
	} else {
		char* buffer = new char[new_size];
		MigrateDiskFileToBuffer(iheader->fstat.st_ino, buffer, new_size);
		UpdateInlineData(myresult, buffer, 0, new_size);
		delete[] buffer;
	}
} else {
	if (new_size > threshold()) {
		int fd;
		if (MigrateToDiskFile(rcbuf, fd, O_TRUNC | O_WRONLY) == 0) {
			if ((ret = ftruncate(fd, new_size)) == 0) {
				fsync(fd);
			}
			close(fd);
		}
	} else {
		TruncateInlineData(myresult, new_size);
	}
}
if (new_size != iheader->fstat.st_size) {
	tfs_inode_header new_iheader = *GetInodeHeader(myresult);
	new_iheader.fstat.st_size = new_size;
	if (new_size > threshold()) {
		new_iheader.has_blob = 1;
	} else {
		new_iheader.has_blob = 0;
	}
	UpdateInodeHeader(myresult, new_iheader);
}
WriteString(&cluster,mykeylist,mdt,myresult);
return ret;
}

int TestFS::Readlink(const char *path, char *buf, size_t size) {

RAMCloud::KeyInfo mykeylist[2];
if (!PathLookup(path, mykeylist)) {
        return FSError("Open: No such file or directory\n");
}

int ret = 0;
ramcloud::Buffer rcbuf;
GetRamCloudBuffer(&cluster,mykeylist,mdt,&rcbuf);
size_t data_size = GetInlineData(rcbuf, buf, 0, size - 1);
buf[data_size] = '\0';
if (ret < 0) {
	return FSError("Open: No such file or directory\n");
} else {
	return 0;
}
}

int TestFS::Symlink(const char *target, const char *path) {
RAMCloud::KeyInfo mykeylist[2];
std::string filename;
if (!PathLookup(path, mykeylist,filename)) {
#ifdef  TABLEFS_DEBUG
	logs->LogMsg("Symlink: %s %s\n", path, target);

#endif
	return FSError("Symlink: No such parent file or directory\n");
}
ramcloud::Buffer rcbuf;
GetRamCloudBuffer(&cluster,mykeylist,mdt,&rcbuf);
size_t val_size = TFS_INODE_HEADER_SIZE + filename.size() + 1 + strlen(target);
char* value = new char[val_size];
tfs_inode_header* header = reinterpret_cast<tfs_inode_header*>(rcbuf.getRange(0,rcbuf.size()));
InitStat(header->fstat, NewInode(), S_IFLNK, 0);
header->has_blob = 0;
header->namelen = filename.size();
char* name_buffer = value + TFS_INODE_HEADER_SIZE;
memcpy(name_buffer, filename.data(), filename.size());
name_buffer[header->namelen] = '\0';
strncpy(name_buffer + filename.size() + 1, target, strlen(target));
std::string towrite(value);
delete[] value;
WriteString(&cluster,mykeylist,mdt,towrite);
return 0;
}

int TestFS::Unlink(const char *path) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("Unlink: %s\n", path);
#endif
RAMCloud::KeyInfo mykeylist[2];
if (!PathLookup(path, mykeylist)) {
        return FSError("Open: No such file or directory\n");
}

int ret = 0;
RAMCloud::Buffer rcbuf;
GetRamCloudBuffer(&cluster,mykeylist,mdt,*rcbuf);
const tfs_inode_header *value = GetInodeHeader(rcbuf);
if (value->fstat.st_size > threshold) {
	char fpath[128];
	GetDiskFilePath(fpath, value->fstat.st_ino);
	unlink(fpath);
}
//To do: move this to tfs_rcdb.cpp
cluster->remove(mdt, mykeylist[0].keyLength, mykeylist[0].keyLength);
return ret;
}

int TestFS::MakeNode(const char *path, mode_t mode, dev_t dev) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("MakeNode: %s\n", path);
#endif
std::string filename;
RAMCloud::KeyInfo mykeylist[2];
if (!PathLookup(path, mykeylist, filename)) {
	return FSError("MakeNode: No such parent file or directory\n");
}

tfs_inode_val_t value = InitInodeValue(NewInode(), mode | S_IFREG, dev,
		filename);

int ret = 0;
WriteString(&cluster,mykeylist,mdt,value);
FreeInodeValue(value);

if (ret == 0) {
	return 0;
} else {
	errno = ENOENT;
	return FSError("MakeNode failed\n");
}
}

int TestFS::MakeDir(const char *path, mode_t mode) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("MakeDir: %s\n", path);
#endif
std::string filename;
RAMCloud::KeyInfo mykeylist[2];
if (!PathLookup(path, mykeylist, filename)) {
        return FSError("MakeDir: No such parent file or directory\n");
}

tfs_inode_val_t value = InitInodeValue(NewInode(), mode | S_IFDIR, 0,
		filename);

int ret = 0;
WriteString(&cluster,mykeylist,mdt,value);
FreeInodeValue(value);

if (ret == 0) {
	return 0;
} else {
	errno = ENOENT;
	return FSError("MakeDir failed\n");
}
}

int TestFS::OpenDir(const char *path, struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("OpenDir: %s\n", path);
#endif
RAMCloud::KeyInfo mykeylist[2];
if (!PathLookup(path, mykeylist)) {
        return FSError("OpenDir: No such parent file or directory\n");
}
fi->keylist_ = mykeylist;
return 0;

}

int TestFS::ReadDir(const char *path, void *buf, fuse_fill_dir_t filler,off_t offset, struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("ReadDir: %s\n", path);
#endif
RAMCloud::mykeylist = fi->keylist_;
RAMCloud::Buffer rcbuf;
GetRamCloudBuffer(&cluster,mykeylist,mdt,*rcbuf);
uint64_t parentid=GetAttribute(rcbuf)->st_ino;
if (filler(buf, ".", NULL, 0) < 0) {
return FSError("Cannot read a directory");
}
if (filler(buf, "..", NULL, 0) < 0) {
return FSError("Cannot read a directory");
}
char* result;
char secondary_key[keylength];
sprintf(secondary_key,"%024lu",parentid);
RAMCloud::IndexKey::IndexKeyRange keyRange(indexId, secondary_key, strlen(secondary_key),secondary_key, strlen(secondary_key));
RAMCloud::IndexLookup rangeLookup(cluster, table, keyRange);
int ret=0;
while (rangeLookup.getNext()) {
	result=static_cast<const char*>(rangeLookup.currentObject()->getValue());
	const char* name_buffer=result+TFS_INODE_HEADER_SIZE;
	if (name_buffer[0] == '\0') {
        	continue;
	}
	if (filler(buf, name_buffer, NULL, 0) < 0) {
		ret = -1;

	};

	if (ret < 0) {
		break;
	}
}
	return ret;
}

int TestFS::ReleaseDir(const char *path, struct fuse_file_info *fi) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("ReleaseDir: %s\n", path);
#endif
tfs_file_handle_t* fh = reinterpret_cast<tfs_file_handle_t*>(fi->fh);
RAMCloud mykeylist = fh->keylist_;
ramcloud::Buffer rcbuf;
int ret=0;
std::string myresult=CopyToString(&cluster,mykeylist,mdt);
const tfs_stat_t *value = GetAttribute(myresult);
tfs_stat_t new_value = *value;
new_value.st_atim.tv_sec = time(NULL);
new_value.st_atim.tv_nsec = 0;
UpdateAttribute(myresult, new_value);

return ret;
}

int TestFS::RemoveDir(const char *path) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("RemoveDir: %s\n", path);
#endif
RAMCloud::KeyInfo mykeylist[2];
if (!PathLookup(path, mykeylist)) {
        return FSError("Open: No such file or directory\n");
}

int ret = 0;
cluster->remove(mdt,mykeylist[0].key,mykeylist[0].keyLength);
}

int TestFS::Rename(const char *old_path, const char *new_path) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("Rename: %s %s\n", old_path, new_path);
#endif
RAMCloud::KeyInfo oldkeylist[2];
RAMCloud::KeyInfo newkeylist[2];
if (!PathLookup(old_path, oldkeylist)) {
return FSError("No such file or directory\n");
}
std::string filename;
if (!PathLookup(new_path, newkeylist, filename)) {
return FSError("No such file or directory\n");
}

#ifdef  TABLEFS_DEBUG
logs->LogMsg("Rename old_key: %s\n", oldkeylist[0].key);
logs->LogMsg("Rename new_key: %s\n", newkeylist[0].key);
#endif

int ret = 0;
std::string myresult=CopyToString(&cluster,mykeylist,mdt);
const tfs_inode_header* old_iheader = GetInodeHeader(myresult);
std::string new_value = InitInodeValue(myresult, filename);
WriteString(&cluster,mykeylist,mdt,new_value);
return ret;
}

int TestFS::Access(const char *path, int mask) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("Access: %s %08x\n", path, mask);
#endif
  //TODO: Implement Access
return 0;
}

int TestFS::UpdateTimens(const char *path, const struct timespec tv[2]) {
#ifdef  TABLEFS_DEBUG
logs->LogMsg("UpdateTimens: %s\n", path);
#endif

RAMCloud::KeyInfo mykeylist[2];
if (!PathLookup(path, mykeylist)) {
        return FSError("OpenDir: No such parent file or directory\n");
}
int ret = 0;
std::string myresult=CopyToString(&cluster,mykeylist,mdt);
const tfs_stat_t *value = GetAttribute(myresult);
tfs_stat_t new_value = *value;
new_value.st_atim.tv_sec = tv[0].tv_sec;
new_value.st_atim.tv_nsec = tv[0].tv_nsec;
new_value.st_mtim.tv_sec = tv[1].tv_sec;
new_value.st_mtim.tv_nsec = tv[1].tv_nsec;
UpdateAttribute(myresult, new_value);
WriteString(&cluster,mykeylist,mdt,myresult);
return ret;
}

int TestFS::Chmod(const char *path, mode_t mode) {
RAMCloud::KeyInfo mykeylist[2];
if (!PathLookup(path, mykeylist)) {
        return FSError("Chmod: No such parent file or directory\n");
}
int ret = 0;
std::string myresult=CopyToString(&cluster,mykeylist,mdt);
const tfs_stat_t *value = GetAttribute(myresult);
tfs_stat_t new_value = *value;
new_value.st_mode = mode;
UpdateAttribute(myresult, new_value);
WriteString(&cluster,mykeylist,mdt,myresult);
return ret;
}

int TestFS::Chown(const char *path, uid_t uid, gid_t gid) {
RAMCloud::KeyInfo mykeylist[2];
if (!PathLookup(path, mykeylist)) {
        return FSError("Chown: No such parent file or directory\n");
}
int ret = 0;
tfs_stat_t new_value = *value;
std::string myresult=CopyToString(&cluster,mykeylist,mdt);
const tfs_stat_t *value = GetAttribute(myresult);
new_value.st_uid = uid;
new_value.st_gid = gid;
UpdateAttribute(myresult, new_value);
WriteString(&cluster,mykeylist,mdt,myresult);
return ret;
}

}
