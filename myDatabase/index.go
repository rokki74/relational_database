package myDatabase

import(
	"bytes"
	"encoding/binary"
)
type IndexHeader struct {
    RootPageId uint32
    TotalPages uint32
    ColumnPos  uint32//for extraction using parts[idx.ColumnPos]
    IsUnique   bool
    KeyType    ColumnType
}

type Index struct {
	Name string
	ColumnPos uint8
	TableId uint16
  FileName string
	MemTree *BPlusTree
}

type BPlusTree struct {
	TreePath *string
	IndexHeader *IndexHeader
	BufferPool *BufferPool
}

type IndexEntry struct {
	Key []byte
	Ptr RowId
}

type NodeType uint8

const (
	INTERNAL NodeType = 1
	LEAF NodeType = 2
)

type NodeHeader struct {
	NodeType NodeType
	KeyCount uint16
	Parent   uint32
}

type InternalNode struct {
	Header NodeHeader
	Keys [][]byte
	Children []uint32
}

type LeafNode struct {
	Header NodeHeader
	//bytes.Compare
	Keys   [][]byte
	Values []RowId
	NextLeaf uint32
}

func (tree *BPlusTree) ReadIndexHeader() *IndexHeader{
	indexHeader := &IndexHeader{}

	page := tree.BufferPool.FetchPage(0, tree.TreePath)
	
	slot := page.read_slot(0)
	offset := slot.offset

	indexHeader.RootPageId = binary.LittleEndian.Uint32(page.data[offset:offset+4])
	offset += 4
	indexHeader.TotalPages = binary.LittleEndian.Uint32(page.data[offset:offset+4])
	offset += 4
	indexHeader.ColumnPos = binary.LittleEndian.Uint32(page.data[offset:offset+4])
	offset += 4

	copy(indexHeader.IsUnique, page.data[offset:offset+1])
	offset += 1 

	//Hoping the next piece was saved with the length of data first
	keyTypeLen := uint8(page.data[offset:offset+1])
	offset += 1
	copy(indexHeader.KeyType, string(page.data[offset:offset+keyTypeLen]))

	return indexHeader
}

func (tree *BPlusTree) Search(key []byte) *RowId {

	node := tree.findLeaf(key)

	for i, k := range node.Keys {
		if bytes.Equal(k, key) {
			return &node.Values[i]
		}
	}

	return nil
}

func (tree *BPlusTree) findLeaf(key []byte) *LeafNode {

	page := tree.BufferPool.FetchPage(tree.IndexHeader.RootPageId, tree.TreePath)

	node := deserializeNode(page)

	for node.Header.NodeType == INTERNAL{

		internal := node.(*InternalNode)

		i := 0
		for i < len(internal.Keys) {
			if bytes.Compare(key, internal.Keys[i]) < 0 {
				break
			}
			i++
		}

		childPage := internal.Children[i]

		page = tree.BufferPool.FetchPage(childPage, tree.TreePath)

		node = deserializeNode(page)
	}

	return node.(*LeafNode)
}

func (tree *BPlusTree) Insert(key []byte, ptr RowId) {

	leaf := tree.findLeaf(key)

	insertIntoLeaf(leaf, key, ptr)

	if leaf.isOverflow() {
		tree.splitLeaf(leaf)
	}
}

func insertIntoLeaf(leaf *LeafNode, key []byte, ptr RowId) {

	i := 0

	for i < len(leaf.Keys) && bytes.Compare(leaf.Keys[i], key) < 0 {
		i++
	}

	leaf.Keys = append(leaf.Keys, nil)
	copy(leaf.Keys[i+1:], leaf.Keys[i:])
	leaf.Keys[i] = key

	leaf.Values = append(leaf.Values, RowId{})
	copy(leaf.Values[i+1:], leaf.Values[i:])
	leaf.Values[i] = ptr
}

func (tree *BPlusTree) splitLeaf(leaf *LeafNode) {

	mid := len(leaf.Keys) / 2

	newLeaf := &LeafNode{}

	newLeaf.Keys = append(newLeaf.Keys, leaf.Keys[mid:]...)
	newLeaf.Values = append(newLeaf.Values, leaf.Values[mid:]...)

	leaf.Keys = leaf.Keys[:mid]
	leaf.Values = leaf.Values[:mid]

	newLeaf.Next = leaf.Next
	leaf.Next = allocateNewPage()

	tree.insertIntoParent(leaf, newLeaf.Keys[0], newLeaf)
}

func (index *Index) BuildMemTreeFromIndexFile(){
	if index.MemTree != nil{
		return
	}

	tree := &BPlusTree{}
	tree.TreePath = &index.FileName
  tree.IndexHeader = tree.ReadIndexHeader()
	
  for pgId=0;pgId<=index.LastPageId;pgId++{
		page := tree.BufferPool.FetchPage(pgId, tree.TreePath)
    header := page.Read_header()
		for s:=0;s<header.RowCount;s++{
		  row := page.Read_row(s)

			indexEntry := &IndexEntry{}
			offset := 0
      switch tree.IndexHeader.KeyType{
				case INT:
					copy(indexEntry.Key, row[offset:offset+4])
					offset += 4
				case STRING:
          copy(indexEntry.Key, row[offset:offset+4])
					offset += 4
				case TIMESTAMP:
					copy(indexEntry.Key, row[offset:offset+8])
					offset += 8
      }


			ptr := RowId{}
			ptr.PageId := binary.LittleEndian.Uint32(row[offset:offset+4])
			offset +=4
			ptr.SlotId := binary.LittleEndian.Uint16(row[offset:offset+2])
			offset += 2

			indexEntry.Ptr = ptr
			//How do i build back the tree here as i already have the IndexEntry either appending it into leaf node and the internal nodes respectively but what is the RootPageId. No using MemTree.Insert is better
			index.MemTree.Insert(indexEntry.Key, indexEntry.Ptr)
		}
	}
}

func (index *Index) DeserializeIndexRow(row []byte){

}


