#include "tfs_rcdb.h"

using namespace TestFS;
int main(){
	RAMCloud::RamCloud cluster("tcp:host=192.168.50.205,port=1101","__unamed__");
	char metatable[]="metatable";
	char idtable[]="idtable";
        uint64_t metaid=cluster.createTable(metatable);
	uint64_t idid=cluster.createTable(idtable);
	metaid=ConnectDB(&cluster,metatable);
	idid=ConnectDB(&cluster,idtable);
	uint64_t nextid=GetNextID(&cluster,idid);
	uint64_t currentid=GetCurrentID(&cluster,idid);
	RAMCloud::KeyInfo mykeylist[2];
	char dirname[]="dir1";
	MakeMetaKey(dirname,0,mykeylist);
	std::string simple_string("mytest");
	tfs_inode_val_t simple_value;
	char teststr[]="mytest";
	simple_value.value=teststr;
	simple_value.size=strlen(teststr);
	WriteString(&cluster, mykeylist,metaid,simple_string);
	WriteString(&cluster, mykeylist,metaid,simple_value);
	RAMCloud::Buffer buffer;
	GetRamCloudBuffer(&cluster,mykeylist[0],metaid,&buffer);
	const char* result=static_cast<const char*>(buffer.getRange(0,buffer.size()));
	printf("%s\n",result);
	std::string mystring=CopytoString(&cluster,mykeylist[0],metaid);
	printf("%s\n",mystring.c_str());



}
