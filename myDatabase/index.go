package myDatabase

import(
	"bytes"
)
type IndexHeader struct {
    RootPageId uint32
    TotalPages uint32
    ColumnPos  uint32//for extraction using parts[idx.ColumnPos] instead of a full schema lookup
    IsUnique   bool
    KeyType    ColumnType
}

type Index struct {
	Name string
	Column string
	TableId uint16
  FileName string
	MemTree *BPlusTree
}

type BPlusTree struct {
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

type LeafNode struct{
	Header NodeHeader
	//bytes.Compare
	Keys   [][]byte
	Values []RowId
	NextLeaf uint32
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

	page := tree.BufferPool.FetchPage(tree.TableId, tree.RootPageId)

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

		page = tree.BufferPool.FetchPage(tree.TableId, childPage)

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

