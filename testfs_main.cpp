#define FUSE_USE_VERSION 26

#include <fuse.h>
#include "fs/tfs_state.h"
#include "fs/testfs.h"
#include "util/properties.h"

static void usage() {
	fprintf(stderr,
			"USAGE:  testfs <FUSEmount> <threshold> <METADIR> <DATADIR> <LOGFILE>\n");
	abort();
}

static TestFS::TestFS *fs;

int wrap_getattr(const char *path, struct stat *statbuf) {
	return fs->GetAttr(path, statbuf);
}
int wrap_readlink(const char *path, char *link, size_t size) {
	return fs->Readlink(path, link, size);
}
int wrap_mknod(const char *path, mode_t mode, dev_t dev) {
	return fs->MakeNode(path, mode, dev);
}
int wrap_mkdir(const char *path, mode_t mode) {
	return fs->MakeDir(path, mode);
}
int wrap_unlink(const char *path) {
	return fs->Unlink(path);
}
int wrap_rmdir(const char *path) {
	return fs->RemoveDir(path);
}
int wrap_symlink(const char *path, const char *link) {
	return fs->Symlink(path, link);
}
int wrap_rename(const char *path, const char *newpath) {
	return fs->Rename(path, newpath);
}
/*
 int wrap_link(const char *path, const char *newpath) {
 return fs->Link(path, newpath);
 }
 */
int wrap_chmod(const char *path, mode_t mode) {
	return fs->Chmod(path, mode);
}
int wrap_chown(const char *path, uid_t uid, gid_t gid) {
	return fs->Chown(path, uid, gid);
}
int wrap_truncate(const char *path, off_t newSize) {
	return fs->Truncate(path, newSize);
}
int wrap_open(const char *path, struct fuse_file_info *fileInfo) {
	return fs->Open(path, fileInfo);
}
int wrap_read(const char *path, char *buf, size_t size, off_t offset,
		struct fuse_file_info *fileInfo) {
	return fs->Read(path, buf, size, offset, fileInfo);
}
int wrap_write(const char *path, const char *buf, size_t size, off_t offset,
		struct fuse_file_info *fileInfo) {
	return fs->Write(path, buf, size, offset, fileInfo);
}
int wrap_release(const char *path, struct fuse_file_info *fileInfo) {
	return fs->Release(path, fileInfo);
}
/*
 int wrap_fsync(const char *path, int datasync, struct fuse_file_info *fi) {
 return fs->Fsync(path, datasync, fi);
 }
 */
int wrap_opendir(const char *path, struct fuse_file_info *fileInfo) {
	return fs->OpenDir(path, fileInfo);
}
int wrap_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
		off_t offset, struct fuse_file_info *fileInfo) {
	return fs->ReadDir(path, buf, filler, offset, fileInfo);
}
int wrap_releasedir(const char *path, struct fuse_file_info *fileInfo) {
	return fs->ReleaseDir(path, fileInfo);
}
void* wrap_init(struct fuse_conn_info *conn) {
	return fs->Init(conn);
}
int wrap_access(const char *path, int mask) {
	return fs->Access(path, mask);
}
int wrap_utimens(const char *path, const struct timespec tv[2]) {
	return fs->UpdateTimens(path, tv);
}
void wrap_destroy(void * data) {
	fs->Destroy(data);
}

static struct fuse_operations testfs_operations;

int main(int argc, char *argv[]) {
	TestFS::Properties prop;
	prop.parseOpts(argc, argv);

	std::string mountdir = prop.getProperty("mountdir");
	std::string datadir = prop.getProperty("datadir");
	if (access(datadir_.c_str(), W_OK) > 0
			|| access(metadir_.c_str(), W_OK) > 0) {
		fprintf(stderr, "cannot open directory!\n");
		exit(1);
	}

	int fuse_stat;
	TestFS::FileSystemState *test_data;
	testfs_data = new testfs::FileSystemState();
	if (testfs_data == NULL || testfs_data->Setup(prop) < 0) {
		fprintf(stdout, "Error allocate testfs_data: %s\n", strerror(errno));
		return -1;
	}

	char *fuse_argv[20];
	int fuse_argc = 0;
	fuse_argv[fuse_argc++] = argv[0];
	char fuse_mount_dir[100];
	strcpy(fuse_mount_dir, mountdir.c_str());
	fuse_argv[fuse_argc++] = fuse_mount_dir;
	fuse_argv[fuse_argc++] = "-s";

	fs = new TestFS::TestFS();
	fs->SetState(testfs_data);

	testfs_opertaions.init = wrap_init;
	testfs_opertaions.getattr = wrap_getattr;
	testfs_opertaions.opendir = wrap_opendir;
	testfs_opertaions.readdir = wrap_readdir;
	testfs_opertaions.releasedir = wrap_releasedir;
	testfs_opertaions.mkdir = wrap_mkdir;
	testfs_opertaions.rmdir = wrap_rmdir;
	testfs_opertaions.rename = wrap_rename;

	testfs_opertaions.symlink = wrap_symlink;
	testfs_opertaions.readlink = wrap_readlink;

	testfs_opertaions.open = wrap_open;
	testfs_opertaions.read = wrap_read;
	testfs_opertaions.write = wrap_write;
	testfs_opertaions.mknod = wrap_mknod;
	testfs_opertaions.unlink = wrap_unlink;
	testfs_opertaions.release = wrap_release;
	testfs_opertaions.chmod = wrap_chmod;
	testfs_opertaions.chown = wrap_chown;

	testfs_opertaions.truncate = wrap_truncate;
	testfs_opertaions.access = wrap_access;
	testfs_opertaions.utimens = wrap_utimens;
	testfs_opertaions.destroy = wrap_destroy;

	fprintf(stdout, "start to run fuse_main at %s %s\n", argv[0],
			fuse_mount_dir);

	fuse_stat = fuse_main(fuse_argc, fuse_argv, &testfs_opertaions,
			testfs_data);

	return fuse_stat;
}
