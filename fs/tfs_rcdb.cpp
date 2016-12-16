#include "tfs_rcdb.h"

namespace TestFS {
#define BIG_CONSTANT(x) (x##LLU)

uint64_t murmur64( const void * key, int len, uint64_t seed )
{
  const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const uint64_t * data = (const uint64_t *)key;
  const uint64_t * end = data + (len/8);

  while(data != end)
  {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const unsigned char * data2 = (const unsigned char*)data;

  switch(len & 7)
  {
  case 7: h ^= uint64_t(data2[6]) << 48;
  case 6: h ^= uint64_t(data2[5]) << 40;
  case 5: h ^= uint64_t(data2[4]) << 32;
  case 4: h ^= uint64_t(data2[3]) << 24;
  case 3: h ^= uint64_t(data2[2]) << 16;
  case 2: h ^= uint64_t(data2[1]) << 8;
  case 1: h ^= uint64_t(data2[0]);
          h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}


char idkey[]="fileid";
uint8_t keylength=25;
uint8_t numKeys=2;

uint64_t ConnectDB(RAMCloud::RamCloud *cluster,char *tablename){
	return cluster->getTableId(tablename);
}

uint64_t GetNextID(RAMCloud::RamCloud *cluster,uint64_t tableid){
	return cluster->incrementInt64(tableid,idkey,strlen(idkey),1);
}

uint64_t GetCurrentID(RAMCloud::RamCloud *cluster,uint64_t tableid){
	RAMCloud::Buffer buf;
	cluster->read(tableid,idkey,strlen(idkey),&buf);
	const uint64_t* myid_p=static_cast<const uint64_t *>(buf.getRange(0,buf.size()));	
	uint64_t myid=*myid_p;
	return myid;
}

int MakeMetaKey(const char* filename, const int len, tfs_inode_t parentid,RAMCloud::KeyInfo *mykeylist){
	char* primary_key=new char[keylength*2];
	char* secondary_key=new char[keylength];
	mykeylist[0].key=primary_key;
	mykeylist[1].key=secondary_key;
	uint64_t hash_id=murmur64(filename, len, 123);
	sprintf(primary_key,"%024lu%025lu",parentid,hash_id);
        sprintf(secondary_key,"%024lu",parentid);
	mykeylist[0].keyLength=strlen(primary_key);
        mykeylist[1].keyLength=strlen(secondary_key);
}

int GetRamCloudBuffer(RAMCloud::RamCloud *cluster, RAMCloud::KeyInfo mykey,uint64_t tableid,RAMCloud::Buffer *buffer){
	cluster->read(tableid,mykey.key,mykey.keyLength,buffer);
	return 0;
}
std::string CopytoString(RAMCloud::RamCloud *cluster,RAMCloud::KeyInfo mykey, uint64_t tableid){
	RAMCloud::Buffer buffer;
	std::string value;
	cluster->read(tableid,mykey.key,mykey.keyLength,&buffer);
	const char* result=static_cast<const char*>(buffer.getRange(0,buffer.size()));
	value=std::string(result);
	return value;
} 
int WriteString(RAMCloud::RamCloud *cluster,RAMCloud::KeyInfo *mykeylist, uint64_t tableid,std::string value)
{ 
	cluster->write(tableid,numKeys,mykeylist,value.data(),value.size());
	return 0;
}
int WriteString(RAMCloud::RamCloud *cluster,RAMCloud::KeyInfo *mykeylist, uint64_t tableid,tfs_inode_val_t inode_val) 
{
	cluster->write(tableid,numKeys,mykeylist,inode_val.value, inode_val.size);
	return 0;
}
}
