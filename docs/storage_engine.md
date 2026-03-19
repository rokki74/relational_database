#The documentation for storage engine.
## 1.INTRODUCTION
# Just incase you haven't read the readme, this is the part i feel could act like a brushing of what it is about the storage engin# Some simple explanations on how i did things -trying to demistify some core functionalities

    I used the offsets which is basically the position a certain data starts at then reading upto it's length.

    Much simpler, bytes or rather a byte is a group of eigth bit which are one's and zero's for example this is a byte: 01111101 -you can replace with whatever bit of your liking make them all zero's for example. Now assume we have a string coming from the user then we turn it into bytes using []byte("the string") then we also capture it's length using len("the string").

    We then create a new array of bytes or can decide to directly write into a page(shall explain shortly what a page is) on one of it's rows specifically then we would have a final variable or a write appearing like this |length of the string|actual data from the string| or rather simpler |len|data|. Then the next time on fetching we fetch this whole array and then read the first part assuming all length was stored in as uint8 then we shall just read into a uint8 variable using either copy() then sorry the other option would have been binary.LittleEndian from the "encoding" package but it only offers implementations from those int of sizes 16 and not 8 so our only available option here is copy which takes in a source and destination, so gave a new variable as destination and only read the array of elements upto 1 byte as i had said earlier 8bits = 1 byte thus uint8 is 1 byte also. SO save this information into a variable maybe call it dataLength, copy(dataLength, byteData[0:1]) using string slices.

    Now our next job would be to use this dataLength to only read the exact data that was saved there and turn it back to string, so before this we need to know dataLength is in byte format so we turn it into uint8 using uint8(dataLength)

    Then we also read the actual data in similar manner using slices also as it is an array but we need to read only upto the offset that is equal to dataLength and also we need to start one byte ahead as that data in that index belongs to another person "dataLength" variable. so simply wewould read it back as binary.LittleEndian.PutUint32(byteData[1:dataLength]) or you can use copy still if you wish.

    Now i can't really explain everything that will be the work of documentation[link-docs]. This was just a part for those who are usually as curious as me so it was just a slight taste into how the implementations were done.

    But really we can assume you've learnt the core of databases, as you have seen i did not use an operating system capabilities as the libraries for programming languages usually provide and just trusted a f.Write("then pass in my data just like that"). No this is exactly one contradicting part operating systems need to ensure the data integrity themselves and also ensure a user can fetch exactly what he saved back by reading that part directly accessing it also enure it is never lost incase of crash. ** You must have realised it it's too deeply nested knowledge that not even i fully understand yet so i just recommend you read books on databases and use online resources for you understanding.


## 2. CONTINUATION OF DOCUMENTATION
* First of all, we start with the most basic feature of a database, this is the page. A page is simply a 4096 sized array of bytes to match how operating system does I/O. The database system converts all data coming in into just bytes including normal columns and strings of data then it translates the bytes back into the original data.
* So the row is a slot inside the page, for each data a user inserts we calculate it's length and make that into a slot, then the next data to come shall be inserted into the next slots which are tracked by indexes called slot indexes. Now the page needs to know where to insert the next data that comes in, thus it stores the offset of where the last write data took space or the last write. Now whenever the page is full as it could only store upto 4097.
/* PAGE HEADER
type PageHeader struct {
	pageId          uint32
	rowCount        uint16
	PageLSN         uint64
	freeSpaceOffset uint16
	overflowPageId  uint32
	flags           uint8
	slotBitMap      [64]byte
}
*/

* Now the table has to find another page to store the data and needs to know which that page is, well we would have used a tree then each page stores the next pageId but no, it's simpler to sequentially assume the file as a series of pages starting from pageId 0 then upto the last page ever used, this way we would always know that the page id zero is at 0 * 4096. Yes i also found it eye revealing, if i had a page Id 5 then i would move my cursor to it's location/offset and read 4096 bytes starting from there then i would know that i have read back my page five! Then the page now has to tracks it's own data the slots or rows it has and the space it still has available writing thus the page header, this page header stores the pageId, dataOffset, numberOfSlots. So one can directly fetch a row he or may need by having the pageId and the slot index so the system fetches that page and uses the slot to fetcht he exact row required, this is used by indexes while full table scan sequentially scan the whole of this file and the whole page for the required data.

