package myDatabase

import (
	"bytes"
	"encoding/binary"
	"strings"
)
type IndexHeader struct {
    RootPageId uint32
    TotalPages uint32
    ColumnPos  uint8//for extraction using parts[idx.ColumnPos]
    IsUnique   bool
    KeyType    ColumnType
}

type Index struct {
	Name string
	ColumnPos uint8
	TableName string
  FileName string
	MemTree *BPlusTree
}

type BPlusTree struct {
	TreePath string
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

func ReadIndexHeader(tree *BPlusTree){
	indexHeader := &IndexHeader{}

	pts := strings.Split(tree.TreePath, ",")
	indexHeadPath :=  pts[0]
	page,_ := tree.BufferPool.FetchPage(0, indexHeadPath+".idx_header")
	
	slot := page.Read_slot(0)
	offset := slot.offset

	indexHeader.RootPageId = binary.LittleEndian.Uint32(page.data[offset:offset+4])
	offset += 4
	indexHeader.TotalPages = binary.LittleEndian.Uint32(page.data[offset:offset+4])
	offset += 4
	indexHeader.ColumnPos = uint8(page.data[offset])
	offset += 1

	indexHeader.IsUnique = page.data[offset] ==1
	offset += 1 

	offset += 1
	indexHeader.KeyType = ColumnType(page.data[offset])

	tree.IndexHeader = indexHeader
}

func (tree *BPlusTree) Search(key []byte) *RowId {
	leaf, _ := tree.findLeaf(key)

	for i, k := range leaf.Keys {
		if bytes.Equal(k, key) {
			return &leaf.Values[i]
		}
	}
	return nil
}

func (tree *BPlusTree) allocatePage() uint32 {
	tree.IndexHeader.TotalPages++
	return tree.IndexHeader.TotalPages - 1
}

func (tree *BPlusTree) findLeaf(key []byte) (*LeafNode, uint32) {
	pageId := tree.IndexHeader.RootPageId

	for {
		page, _ := tree.BufferPool.FetchPage(pageId, tree.TreePath)
		node, ndType := DeserializeNode(page)

		if ndType == LEAF {
			return node.(*LeafNode), pageId
		}

		internal := node.(*InternalNode)

		i := 0
		for i < len(internal.Keys) && bytes.Compare(key, internal.Keys[i]) >= 0 {
			i++
		}

		pageId = internal.Children[i]
	}
}

func (tree *BPlusTree) Insert(key []byte, ptr RowId) {
	leaf, pageId := tree.findLeaf(key)

	tree.insertIntoLeaf(leaf, pageId, key, ptr)
}

func DeserializeInternalNode(page *Page) *InternalNode{
	data := page.data
	offset := 0

	internal := &InternalNode{}

	// Header
	internal.Header.NodeType = NodeType(data[offset])
	offset += 1

	internal.Header.KeyCount = binary.LittleEndian.Uint16(data[offset:offset+2])
	offset += 2

	internal.Header.Parent = binary.LittleEndian.Uint32(data[offset:offset+4])
	offset += 4

	// Keys
	for i := 0; i < int(internal.Header.KeyCount)+1; i++ {
		keyLen := int(data[offset])
		offset += 1

		key := make([]byte, keyLen)
		copy(key, data[offset:offset+keyLen])
		offset += keyLen

		internal.Keys = append(internal.Keys, key)
	}

	children := make([]uint32,0)
	// Values
	for i := 0; i < int(internal.Header.KeyCount); i++ {
		pageId := binary.LittleEndian.Uint32(data[offset:offset+4])
		offset += 4
    children = append(children, pageId) 
	}

	internal.Children = children
	return internal
}

func DeserializeLeafNode(page *Page) *LeafNode {
	data := page.data
	offset := 0

	leaf := &LeafNode{}

	// Header
	leaf.Header.NodeType = NodeType(data[offset])
	offset += 1

	leaf.Header.KeyCount = binary.LittleEndian.Uint16(data[offset:offset+2])
	offset += 2

	leaf.Header.Parent = binary.LittleEndian.Uint32(data[offset:offset+4])
	offset += 4

	leaf.NextLeaf = binary.LittleEndian.Uint32(data[offset:offset+4])
	offset += 4

	// Keys
	for i := 0; i < int(leaf.Header.KeyCount); i++ {
		keyLen := int(data[offset])
		offset += 1

		key := make([]byte, keyLen)
		copy(key, data[offset:offset+keyLen])
		offset += keyLen

		leaf.Keys = append(leaf.Keys, key)
	}

	// Values
	for i := 0; i < int(leaf.Header.KeyCount); i++ {
		rowId := RowId{}

		rowId.PageId = binary.LittleEndian.Uint32(data[offset:offset+4])
		offset += 4

		rowId.SlotId = binary.LittleEndian.Uint16(data[offset:offset+2])
		offset += 2

		leaf.Values = append(leaf.Values, rowId)
	}

	return leaf
}

func (tree *BPlusTree) writeInternal(pageId uint32, node *InternalNode) {
	page, _ := tree.BufferPool.FetchPage(pageId, tree.TreePath)

	offset := 0

	page.data[offset] = byte(INTERNAL)
	offset += 1

	binary.LittleEndian.PutUint16(page.data[offset:], uint16(len(node.Keys)))
	offset += 2

	binary.LittleEndian.PutUint32(page.data[offset:], node.Header.Parent)
	offset += 4

	// keys
	for _, key := range node.Keys {
		page.data[offset] = byte(len(key))
		offset += 1

		copy(page.data[offset:], key)
		offset += len(key)
	}

	// children
	for _, child := range node.Children {
		binary.LittleEndian.PutUint32(page.data[offset:], child)
		offset += 4
	}

	tree.BufferPool.SavePage(tree.TreePath, *page)
}

func (tree *BPlusTree) writeLeaf(pageId uint32, leaf *LeafNode) {
	page, _ := tree.BufferPool.FetchPage(pageId, tree.TreePath)

	offset := 0

	page.data[offset] = byte(LEAF)
	offset += 1

	binary.LittleEndian.PutUint16(page.data[offset:], uint16(len(leaf.Keys)))
	offset += 2

	binary.LittleEndian.PutUint32(page.data[offset:], leaf.Header.Parent)
	offset += 4

	binary.LittleEndian.PutUint32(page.data[offset:], leaf.NextLeaf)
	offset += 4

	// keys
	for _, key := range leaf.Keys {
		page.data[offset] = byte(len(key))
		offset += 1

		copy(page.data[offset:], key)
		offset += len(key)
	}

	// values
	for _, val := range leaf.Values {
		binary.LittleEndian.PutUint32(page.data[offset:], val.PageId)
		offset += 4

		binary.LittleEndian.PutUint16(page.data[offset:], val.SlotId)
		offset += 2
	}

	tree.BufferPool.SavePage(tree.TreePath, *page)
}

func (tree *BPlusTree) insertIntoLeaf(leaf *LeafNode, pageId uint32, key []byte, ptr RowId) {

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

	if tree.isLeafOverflow(leaf) {
		tree.splitLeaf(leaf, pageId)
	} else {
		tree.writeLeaf(pageId, leaf)
	}
}

const MAX_KEYS = 128 // depends on page size

func (tree *BPlusTree) isInternalOverflow(node *InternalNode) bool {
	return len(node.Keys) > MAX_KEYS
}

func (tree *BPlusTree) isLeafOverflow(leaf *LeafNode) bool {
	return len(leaf.Keys) > MAX_KEYS
}

func (tree *BPlusTree) insertIntoParent(leftPage uint32, key []byte, rightPage uint32) {

	leftNodePage, _ := tree.BufferPool.FetchPage(leftPage, tree.TreePath)
	leftNode, nodeType:= DeserializeNode(leftNodePage)

	var parentId uint32

	if nodeType == LEAF {
		parentId = leftNode.(*LeafNode).Header.Parent
	} else {
		parentId = leftNode.(*InternalNode).Header.Parent
	}

	// CASE 1: new root
	if parentId == 0 {
		newRootId := tree.allocatePage()

		root := &InternalNode{
			Header: NodeHeader{
				NodeType: INTERNAL,
			},
			Keys:     [][]byte{key},
			Children: []uint32{leftPage, rightPage},
		}

		tree.IndexHeader.RootPageId = newRootId

		tree.writeInternal(newRootId, root)
		return
	}

	// CASE 2: insert into existing parent
	parentPage, _ := tree.BufferPool.FetchPage(parentId, tree.TreePath)
	node,_ := DeserializeNode(parentPage)
  
  parent := node.(*InternalNode)
	i := 0
	for i < len(parent.Children) && parent.Children[i] != leftPage {
		i++
	}

	parent.Keys = append(parent.Keys, nil)
	copy(parent.Keys[i+1:], parent.Keys[i:])
	parent.Keys[i] = key

	parent.Children = append(parent.Children, 0)
	copy(parent.Children[i+2:], parent.Children[i+1:])
	parent.Children[i+1] = rightPage

	if tree.isInternalOverflow(parent) {
		tree.splitInternal(parent, parentId)
	} else {
		tree.writeInternal(parentId, parent)
	}
}

func (tree *BPlusTree) splitInternal(node *InternalNode, pageId uint32) {

	mid := len(node.Keys) / 2
	promoteKey := node.Keys[mid]

	newNode := &InternalNode{
		Header: NodeHeader{
			NodeType: INTERNAL,
			Parent:   node.Header.Parent,
		},
	}

	// right side
	newNode.Keys = append(newNode.Keys, node.Keys[mid+1:]...)
	newNode.Children = append(newNode.Children, node.Children[mid+1:]...)

	// left side stays
	node.Keys = node.Keys[:mid]
	node.Children = node.Children[:mid+1]

	// allocate new page
	newPageId := tree.allocatePage()

	// write both
	tree.writeInternal(pageId, node)
	tree.writeInternal(newPageId, newNode)

	// FIX CHILDREN PARENT POINTERS
	for _, childId := range newNode.Children {
		childPage, _ := tree.BufferPool.FetchPage(childId, tree.TreePath)
		childNode, nodeType := DeserializeNode(childPage)

		if nodeType == LEAF {
			childNode.(*LeafNode).Header.Parent = newPageId
			tree.writeLeaf(childId, childNode.(*LeafNode))
		} else {
			childNode.(*InternalNode).Header.Parent = newPageId
			tree.writeInternal(childId, childNode.(*InternalNode))
		}
	}

	// push up
	tree.insertIntoParent(pageId, promoteKey, newPageId)
}

func (tree *BPlusTree) splitLeaf(leaf *LeafNode, pageId uint32) {

	mid := len(leaf.Keys) / 2

	newLeaf := &LeafNode{
		Header: NodeHeader{
			NodeType: LEAF,
			Parent:   leaf.Header.Parent,
		},
	}

	newLeaf.Keys = append(newLeaf.Keys, leaf.Keys[mid:]...)
	newLeaf.Values = append(newLeaf.Values, leaf.Values[mid:]...)

	leaf.Keys = leaf.Keys[:mid]
	leaf.Values = leaf.Values[:mid]

	// allocate new page
	newPageId := tree.allocatePage()

	// fix linked list
	newLeaf.NextLeaf = leaf.NextLeaf
	leaf.NextLeaf = newPageId

	// write both pages
	tree.writeLeaf(pageId, leaf)
	tree.writeLeaf(newPageId, newLeaf)

	// promote key
	promoteKey := newLeaf.Keys[0]

	tree.insertIntoParent(pageId, promoteKey, newPageId)
}

func DeserializeNode(page *Page) (interface{}, NodeType) {
	nodeType := NodeType(page.data[0])

	if nodeType == LEAF {
		return DeserializeLeafNode(page), LEAF
	}
	return DeserializeInternalNode(page), INTERNAL
}

func (index *Index) BuildMemTreeFromIndexFile(){
	if index.MemTree != nil{
		return
	}

	tree := &BPlusTree{}
	tree.TreePath = index.FileName
  ReadIndexHeader(tree)
	
	for pgId:=0; pgId <= int(index.MemTree.IndexHeader.TotalPages)-1; pgId++{
		page, _ := tree.BufferPool.FetchPage(uint32(pgId), tree.TreePath)
    header := page.Read_header()
		for s:=0;s<int(header.RowCount);s++{
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
				/*case TIMESTAMP:
					copy(indexEntry.Key, row[offset:offset+8])
					offset += 8
					*/
      }


			ptr := RowId{}
			ptr.PageId = binary.LittleEndian.Uint32(row[offset:offset+4])
			offset +=4
			ptr.SlotId = binary.LittleEndian.Uint16(row[offset:offset+2])
			offset += 2

			indexEntry.Ptr = ptr
			//How do i build back the tree here as i already have the IndexEntry either appending it into leaf node and the internal nodes respectively but what is the RootPageId. No using MemTree.Insert is better
			index.MemTree.Insert(indexEntry.Key, indexEntry.Ptr)
		}
	}
}

func (tree *BPlusTree) Delete(key []byte, rowId RowId) {

	leaf, pageId := tree.findLeaf(key)

	for i, k := range leaf.Keys {
		if bytes.Equal(k, key) && leaf.Values[i] == rowId {

			leaf.Keys = append(leaf.Keys[:i], leaf.Keys[i+1:]...)
			leaf.Values = append(leaf.Values[:i], leaf.Values[i+1:]...)

			tree.writeLeaf(pageId, leaf)
			return
		}
	}
}



