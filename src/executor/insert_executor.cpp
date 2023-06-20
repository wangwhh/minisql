//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"
#include "executor/plans/values_plan.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  CatalogManager *catalog = exec_ctx_->GetCatalog();
  TableInfo *table;
  assert(catalog->GetTable(plan_->GetTableName(), table) == DB_SUCCESS);
  table_heap_ = table->GetTableHeap();
  exec_ctx_->GetCatalog()->GetTableIndexes(table->GetTableName(), index_info_);
  unique_index_ = nullptr;
  schema_ = table->GetSchema();
  for(auto index : index_info_){
    if(index->GetIndexName() == "unique"){
      unique_index_ = index->GetIndex();
      break;
    }
  }
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  vector<Field> fields;
  ValuesPlanNode *values_plan_node = (ValuesPlanNode *)plan_->GetChildPlan().get();
  auto values = values_plan_node->values_;
  for(auto value : values){
    for(auto field_ptr : value){
      Field field = field_ptr->Evaluate(row);
      fields.push_back(field);
    }
  }

  Row tuple(fields);
  if(!table_heap_->InsertTuple(tuple, exec_ctx_->GetTransaction())){
    cout << "The tuple is too large." << endl;
  }

  // 插入index
  vector<pair<IndexInfo *, Row>> inserted_entry;
  for(auto index : index_info_){
    Schema *schema = index->GetIndexKeySchema();
    vector<Field> index_entry;
    // 投影
    for(auto column : schema->GetColumns()){
      uint32_t col_idx;
      schema_->GetColumnIndex(column->GetName(), col_idx);
      index_entry.push_back(*tuple.GetField(col_idx));
    }
    Row key(index_entry);
    key.SetRowId(tuple.GetRowId());
    // unique检测
    if(index->GetIndexName().find("unique")!= string::npos  || index->GetIndexName() == "primary"){
      vector<RowId> duplicate;
      index->GetIndex()->ScanKey(key, duplicate, exec_ctx_->GetTransaction());
      if(!duplicate.empty()){
        cout << "Duplicated insert!" << endl;
        table_heap_->ApplyDelete(tuple.GetRowId(), exec_ctx_->GetTransaction());
        while(!inserted_entry.empty()){
          auto del_entry = inserted_entry.begin();
          del_entry->first->GetIndex()->RemoveEntry(del_entry->second, del_entry->second.GetRowId(), exec_ctx_->GetTransaction());
          inserted_entry.erase(del_entry);
        }
        return false;
      }
    }

    key.SetRowId(tuple.GetRowId());
    index->GetIndex()->InsertEntry(key, tuple.GetRowId(), exec_ctx_->GetTransaction());
    inserted_entry.emplace_back(index, key);
  }
  return false;
}