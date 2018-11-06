/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {

  //A array holding information about each buffer frame  
	bufDescTable = new BufDesc[bufs];


  for (FrameId i = 0; i < bufs; i++) 
  {
    //Set the valid bit to false initially
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  //An array of pages brought in memory from the disk
  //correspondence b/w bufPool and bufDescTable shows
  //what page is pinned to which frame
  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
}

//Move the hand of the clock to the next frame
void BufMgr::advanceClock()
{
  //current index % bufs - 1
  clockHand = clockhand % (bufs - 1);
}

//Will try to return an empty frame from the memory pool,
void BufMgr::allocBuf(FrameId & frame) 
{

  //Keep looking for a page
  while(true){
    advanceClock();
    //Check if valid bit is set
    if(bufDescTable[clockHand].valid){
      //Check if reference bit is set or no
      if(bufDescTable[clockHand].refBit){
        //Clear the ref bit
        bufDescTable[clockHand].refBit = false;
        continue;
      }

      if(bufDescTable[clockHand].pinCnt > 0){
        continue;
      }

      //Check if dirty bit is set
      if(bufDescTable[clockHand].dirty){
        //Flush this particular page to disk
      }

    }

    //Call set() on this particular frame to clear it up
    bufDescTable[clockHand].

    //return that frame's pointer

  }
}




}

//Read page
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{

  //Check if file name, page num are in our hashtable
  hashTable.lookup(file, pageNo, )

}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
