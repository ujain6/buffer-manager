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
using namespace std;
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
  cout << "Creating a buffer pool of " << numBufs << " frames" <<endl;
	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
}

//Move the hand of the clock to the next frame
void BufMgr::advanceClock()
{
  clockHand = (clockHand + 1)%numBufs;
}

//Will try to return an empty frame from the memory pool,
void BufMgr::allocBuf(FrameId & frame)
{
  cout<<"Trying to find empty frame ------" << endl;
  //Keep looking for an empty frame, if after a sweep, all pages in memory
	//are pinned, throw the BufferExceededException
	int start = (clockHand + 1)%numBufs;
	int count = 0;
	cout <<" Clock hand at the starting position " << start << endl;
	int numPinnedPages = 0;
  while(true){

		advanceClock();
		BufDesc * currFrame = &bufDescTable[clockHand];
		cout<<"Considering frame #"<< currFrame->frameNo <<endl;
		if(clockHand == start){
			count++;
		}
		//If we are at starting position, and all pages are
		//pinned
		if(count >= 2 && numPinnedPages == numBufs){
			cout<<" ******Clock hand at the starting position again*******" << start;
			//Throw BufferExceededException
			throw BufferExceededException();
		}

    //Check if valid bit is set
    if(currFrame->valid){
      //Check if reference bit is set or no
      if(currFrame->refbit){
        //Clear the ref bit
        currFrame->refbit = false;
        continue;
      }
			//Referece bit was set to false, so we will pick this page is not
			//pinned by any process
      if(currFrame->pinCnt > 0){
				//cout<<"Frame still being used, moving to next frame" << endl;
				numPinnedPages++;
        continue;
      }
			//Finally, this frame was not referenced recently nor it is pinned, evict
			//this page
      //Check if dirty bit is set
      if(currFrame->dirty){
        //Flush this particular page to disk
				currFrame->file->writePage(bufPool[currFrame->frameNo]);
      }

    }

		frame = currFrame->frameNo;
		//cout << "Using frame " << frame << endl;
		//If file pointer is not NULL
		if(currFrame->file != NULL){
			//Removing the evicted entry from our hashmap
			hashTable->remove(currFrame->file, currFrame->pageNo);
			cout << " Kicking out " << currFrame->file->filename() << " , "
			  << currFrame->pageNo << endl;
		}

		currFrame->Clear();
		break; //Move out of this loop
  }
}


//Read page
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try
	{

		hashTable->lookup(file, pageNo, frameNo);
		BufDesc * frame = &bufDescTable[frameNo];
		frame->pinCnt++;
		frame->refbit = true;
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
		bufPool[frameNo] = p;
		page = &bufPool[frameNo];
	}

}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{

	FrameId frameNo;
	try
	{
		hashTable->lookup(file, pageNo, frameNo);
		BufDesc *frame = &(bufDescTable[frameNo]);
		if(frame->pinCnt == 0)
		{
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		}

		frame->pinCnt--;

		if(dirty)
		{
			frame->dirty = true;
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

	//Check if this frame holds the page for this file
	for(uint32_t i = 0; i < numBufs; i++){
		BufDesc * frame = &bufDescTable[i];
		if(frame->file->filename() == file->filename()){

			if(frame->pinCnt > 0)
			{
				throw PagePinnedException(file->filename(), frame->pageNo, frame->frameNo);
			}
			if(!frame->valid)
			{
				throw BadBufferException(frame->frameNo, frame->dirty, frame->valid, frame->refbit);
			}
			if(frame->dirty){
				frame->file->writePage(bufPool[frame->frameNo]);
				frame->dirty = false;
			}
			//Remove this particular file, page # mapping from the hashmap
			hashTable->remove(file, frame->pageNo);
			frame->Clear();
		}

	}

}

//
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page)
{
	FrameId frameNo;
	Page p = file->allocatePage();
	allocBuf(frameNo);
	cout << "Pinning new page to frame number " << frameNo << endl;
	//Assign the corresponding page to the frameNo
	bufPool[frameNo] = p;
	pageNo = p.page_number();
	hashTable->insert(file, pageNo, frameNo);
	//Calling set will give this frame the details of this page that is being alloted
	bufDescTable[frameNo].Set(file, pageNo);

	cout << "Page number allocated is " << pageNo << endl;
	cout << "----------------------" << endl;

	//Return a pointer to that page

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
