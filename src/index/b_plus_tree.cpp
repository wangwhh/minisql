#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  auto *indexRootsPage = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if(!indexRootsPage->GetRootId(index_id, &root_page_id_)){
    root_page_id_ = INVALID_PAGE_ID;
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
  BPlusTreeLeafPage *root_page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->FetchPage(root_page_id_));
  root_page->Init(root_page_id_);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(current_page_id);
  auto *node = reinterpret_cast<BPlusTreePage *>(page);
  if(node->IsLeafPage()){ // 如果是叶子，直接删
    buffer_pool_manager_->UnpinPage(current_page_id, true);
    buffer_pool_manager_->DeletePage(current_page_id);
    return;
  }else{  // 不是的话，先删孩子，然后删掉这一页
    for(int i = 0; i < node->GetSize(); i++){
      auto *internalPage = reinterpret_cast<BPlusTreeInternalPage *>(node);
      page_id_t child_id = internalPage->ValueAt(i);
      Destroy(child_id);
    }
    buffer_pool_manager_->UnpinPage(current_page_id, true);
    buffer_pool_manager_->DeletePage(current_page_id);
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  Page *page = FindLeafPage(key);
  assert(page != nullptr);
  auto *leaf = reinterpret_cast<BPlusTreeLeafPage *>(page->GetData());
  RowId rowId;
  bool is_find = leaf->Lookup(key, rowId, processor_);
  if(is_find){
    result.push_back(rowId);
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return is_find;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }else{
    return InsertIntoLeaf(key, value, transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  Page *root_page = buffer_pool_manager_->NewPage(root_page_id_);
  auto leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(root_page->GetData());
  leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page->Insert(key, value, processor_);
  UpdateRootPageId(1);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  Page *leaf_page = FindLeafPage(key);
  auto leaf_node = reinterpret_cast<BPlusTreeLeafPage *>(leaf_page->GetData());
  int pre_size = leaf_node->GetSize();
  int insert_size = leaf_node->Insert(key, value, processor_);
  if(pre_size == insert_size){  // 重复插入
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  if(insert_size > leaf_max_size_){ // 需要分裂
    BPlusTreeLeafPage *new_page = Split(leaf_node, transaction);
    InsertIntoParent(leaf_node, new_page->KeyAt(0), new_page, transaction);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    UpdateRootPageId(0);
  }else{
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  auto new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
  new_node->Init(new_page_id, node->GetParentPageId(), 0, internal_max_size_);
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  auto new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_node->Init(new_page_id, node->GetParentPageId(), 0, leaf_max_size_);
  node->MoveHalfTo(new_node);
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  if(old_node->IsRootPage()){
    Page *new_page = buffer_pool_manager_->NewPage(root_page_id_);
    auto *new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, 0, internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    UpdateRootPageId(0);
    return;
  }else{
    Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    int new_size = parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if(new_size >= internal_max_size_){  // 需要分裂
      auto split_node = Split(parent_node, transaction); // 分裂后的结点
      GenericKey *new_key = split_node->KeyAt(0);
      InsertIntoParent(parent_node, new_key, split_node, transaction);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(split_node->GetPageId(), true);
    }else{
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  Page *leaf_page = FindLeafPage(key);
  auto *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int pre_size = leaf_node->GetSize();
  int new_size = leaf_node->RemoveAndDeleteRecord(key, processor_);
  if(new_size == pre_size){ // 删除失败，不存在
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  }else{
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    bool del = CoalesceOrRedistribute(leaf_node, transaction);
    if(del){
      buffer_pool_manager_->DeletePage(leaf_node->GetPageId());
    }
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  if(node->IsRootPage()){
    return AdjustRoot(node);
  }else if(node->GetSize() >= node->GetMinSize()){
    return false;
  }

  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int parent_index = parent_node->ValueIndex(node->GetPageId());
  int sibling_index;
  if(parent_index == 0){
    sibling_index = 1;
  }else{
    sibling_index = parent_index - 1;
  }
  page_id_t sibling_page_id = parent_node->ValueAt(sibling_index);
  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_page_id);

  N *sibling_node = reinterpret_cast<N*>(sibling_page->GetData());
  if(sibling_node->GetSize() + node->GetSize() >= node->GetMaxSize()){
    Redistribute(sibling_node, node, parent_index);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false; // no deletion
  }else{
    bool parent_need_del = Coalesce(sibling_node, node, parent_node, parent_index, transaction);
    if(parent_need_del){
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(parent_node->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return true;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  if(index == 0){ // neighbor在右边
    Page *first_page = FindLeafPage(node->KeyAt(1), INVALID_PAGE_ID, true);
    LeafPage *first_node = reinterpret_cast<LeafPage*>(first_page->GetData());
    if(node->GetPageId() != first_node->GetPageId()){
      while(true){
        if(first_node->GetNextPageId() == node->GetPageId()){
          first_node->SetNextPageId(neighbor_node->GetPageId());
          break;
        }
        page_id_t next_page_id = first_node->GetNextPageId();
        Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
        LeafPage *next_node = reinterpret_cast<LeafPage *>(next_page->GetData());
        buffer_pool_manager_->UnpinPage(next_page_id, false);
        first_node = next_node;
      }
    }
    buffer_pool_manager_->UnpinPage(first_page->GetPageId(), false);
    node->MoveAllTo(neighbor_node);
    // first是value还是key?
    parent->SetKeyAt(1, parent->KeyAt(0));

  }else{
    node->MoveAllTo(neighbor_node);
  }
  parent->Remove(index);
  return CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  if(index == 0){
    node->MoveAllTo(neighbor_node, parent->KeyAt(1), buffer_pool_manager_);
    parent->SetKeyAt(1, parent->KeyAt(0));
  }else{
    node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);

  }
  parent->Remove(index);
  return CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  page_id_t parent_page_id = node->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page->GetData());
  if(index == 0){ // 右边
    neighbor_node->MoveFirstToEndOf(node);
    parent_page->SetKeyAt(1, neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }else{    //左边
    neighbor_node->MoveLastToFrontOf(node);
    parent_page->SetKeyAt(index, node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  page_id_t parent_page_id = node->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_page_id);
  InternalPage *parent_page = reinterpret_cast<InternalPage *>(page->GetData());
  if(index == 0){
    neighbor_node->MoveFirstToEndOf(node, parent_page->KeyAt(1), buffer_pool_manager_);
    parent_page->SetKeyAt(1, neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }else{
    GenericKey *mid_key = neighbor_node->KeyAt(neighbor_node->GetSize()-1);
    neighbor_node->MoveLastToFrontOf(node, mid_key, buffer_pool_manager_);
    parent_page->SetKeyAt(index, node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
  }
}


/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if(!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1){
    //case 1
    auto old_root = reinterpret_cast<InternalPage *>(old_root_node);
    // 孩子变root
    page_id_t child_page_id = old_root->RemoveAndReturnOnlyChild();
    root_page_id_ = child_page_id;
    UpdateRootPageId(0);
  }else if(old_root_node->IsLeafPage() && old_root_node->GetSize() == 0){
    // case 2 直接删树
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
    return true;
  }
  buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  Page *page = FindLeafPage(nullptr, root_page_id_, true);
  LeafPage *node = reinterpret_cast<LeafPage *>(page->GetData());
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  return IndexIterator(node->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  Page *page = FindLeafPage(key, root_page_id_);
  LeafPage *node = reinterpret_cast<LeafPage *>(page->GetData());
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  int index = node->KeyIndex(key, processor_);
  return IndexIterator(node->GetPageId(), buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  Page *page = FindLeafPage(nullptr, root_page_id_, false);
  LeafPage *node = reinterpret_cast<LeafPage *>(page->GetData());
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  return IndexIterator(node->GetPageId(), buffer_pool_manager_, node->GetSize());
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if(page_id == INVALID_PAGE_ID) page_id = root_page_id_;
  Page *cur_page = buffer_pool_manager_->FetchPage(page_id);
  buffer_pool_manager_->UnpinPage(page_id, false);
  BPlusTreePage *cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  while(!cur_node->IsLeafPage()){
    InternalPage *inter_node = reinterpret_cast<InternalPage *>(cur_node);
    page_id_t child_page_id;
    if(leftMost || key == nullptr){
      child_page_id = inter_node->ValueAt(0);
    }else{
      child_page_id = inter_node->Lookup(key, processor_);
    }
    cur_page = buffer_pool_manager_->FetchPage(child_page_id);
    cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
    buffer_pool_manager_->UnpinPage(child_page_id, false);
  }
  return cur_page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  Page *root_page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage *root_node = reinterpret_cast<IndexRootsPage *>(root_page);
  if(insert_record == 0){
    // update
    root_node->Update(index_id_, root_page_id_);
  }else{
    root_node->Insert(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}