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
  clockHand = (clockhand + 1)%numBufs;
}

//Will try to return an empty frame from the memory pool,
void BufMgr::allocBuf(FrameId & frame)
{

  //Keep looking for an empty frame, if after a sweep, all pages in memory
	//are pinned, throw the BufferExceededException
	int start = clockHand + 1;
	int count = 0;

  while(true){

		advanceClock();
		if(clockHand == start){
			count++;
		}

		if(count == 2){
			//Throw BufferExceededException
			throw BufferExceededException();
			break;
		}
		//If after advancing, we come to start, increment counter

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

		frame = bufDescTable[clockHand];
		break; //Move out of this loop
  }

}

}

//Read page
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try
	{
		//if page is in buffer pool
		hashTable->lookup(file, pageNo, frameNo);
		BufDesc frame = bufDescTable[frameNo];
		frame.pinCnt++;
		frame.refbit = true;
		page = &bufPool[frameNo];
	}
	//if page is not in buffer pool
	catch (HashNotFoundException e)
	{
		//reading page from disk into buffer pool frame
		allocBuf(frameNo);
		Page p = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frameNo);
		bufDescTable[frameNo].Set(file, pageNo);
		page = &bufPool[frameNo];
	}

}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
	FrameId frameNo;
	bool found = false;
	try
	{
		hashTable->lookup(file, pageNo, frameNo);
		BufDesc frame = bufDescTable[frameNo];
		if(frame.pinCnt == 0)
		{
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		}
		frame.pinCnt-=1;
		if(dirty)
		{
			frame.dirty = true;
		}

	}
	catch (HashNotFoundException e)
	{
		//Do nothing if the entry is not found in the buffer
		return;
	}

}

void BufMgr::flushFile(const File* file)
{
}

//
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
	FrameId frameNo;
	Page p = file->allocatePage();
	allocBuf(frameNo);
	bufPool[frameNo] = p;
	hashTable->insert(file, p.page_number(), frameNo);
	bufDescTable[frameNo].Set(file, p.page_number());
	pageNo = p.page_number();
	page = &bufPool[frameNo];

}

void BufMgr::disposePage(File* file, const PageId PageNo)
{

	FrameId frameNo;
	hashTable->lookup(file, PageNo, frameNo);
	hashTable->remove(file,PageNo);
	bufDescTable[frameNo].Clear();
	file->deletePage(PageNo);

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
