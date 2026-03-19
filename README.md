# relational_database
A relational database management system built using golang, designed to be compatible with sql uses sql queries and similar concepts  and builds to postgres

# purpose
This project has brought about my understanding of database and also other system principles it interacts with including networking and some phases of how the operating system works under the hood.
The project can't be a complete replacement to existent ones but benchmarking it against them is my ultimate mission and then switching my other programs to test it with them also so i discover the flows i might have had.
With this project i have learnt concepts on low level utilities like byte serialization and desirialization, in simpler terms i was able to turn the data that comes from the user into bytes then save it on the disk efficiently, well the only problem would be how to translate it back.

# Some simple explanations on how i did things -trying to demistify some core functionalities
* so i used the offsets which is basically the position a certain data starts at then reading upto it's length.
* Much simpler, bytes or rather a byte is a group of eigth bit which are one's and zero's for example this is a byte: 01111101 -you can replace with whatever bit of your liking make them all zero's for example.
Now assume we have a string coming from the user then we turn it into bytes using []byte("the string") then we also capture it's length using len("the string").
* We then create a new array of bytes or can decide to directly write into a page(shall explain shortly what a page is) on one of it's rows specifically then we would have a final variable or a write appearing like this |length of the string|actual data from the string| or rather simpler |len|data|. 
Then the next time on fetching we fetch this whole array and then read the first part assuming all length was stored in as uint8 then we shall just read into a uint8 variable using either copy() then sorry the other option would have been binary.LittleEndian from the "encoding" package but it only offers implementations from those int of sizes 16 and not 8 so our only available option here is copy which takes in a source and destination, so gave a new variable as destination and only read the array of elements upto 1 byte as i had said earlier 8bits = 1 byte thus uint8 is 1 byte also. SO save this information into a variable maybe call it dataLength, copy(dataLength, byteData[0:1]) using string slices.
* Now our next job would be to use this dataLength to only read the exact data that was saved there and turn it back to string, so before this we need to know dataLength is in byte format so we turn it into uint8 using uint8(dataLength)
* Then we also read the actual data in similar manner using slices also as it is an array but we need 
to read only upto the offset that is equal to dataLength and also we need to start one byte ahead as that data in that index belongs to another person "dataLength" variable. so simply wewould read it back as binary.LittleEndian.PutUint32(byteData[1:dataLength]) or you can use copy still if you wish.

* Now i can't really explain everything that will be the work of documentation[link-docs]. This was just a part for those who are usually as curious as me so it was just a slight taste into how the implementations were done.
* But really we can assume you've learnt the core of databases, as you have seen i did not use an operating system capabilities as the libraries for programming languages usually provide and just trusted a f.Write("then pass in my data just like that"). No this is exactly one contradicting part operating systems need to ensure the data integrity themselves and also ensure a user can fetch exactly what he saved back by reading that part directly accessing it also enure it is never lost incase of crash.
** You must have realised it it's too deeply nested knowledge that not even i fully understand yet so i just recommend you read books on databases and use online resources for you understanding.

# Back to our earlier focus -Have a little patience and next i shall be having the promised benchmarks here in this section including details of what happened or what went wrong when i used the system to save my data from my other programs and the flaws.

* ALL CRITICS ARE WELCOME TELL ME WHAT I DID WRONG/GOOD OR HOW I CAN IMPROVE THE SYSTEM ALSO DEVS ARE WELCOME INTO MY documentation-link HELP MAKE EASIER FOR OTHER DEVELOPERS TO UNDERTAND THE SYSTEM INCASE YOU HAVE GONE THROUGH IT. 
Thank You!


