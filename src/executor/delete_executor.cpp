//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_);
  heap_ = table_->GetTableHeap();
  exec_ctx_->GetCatalog()->GetTableIndexes(plan_->GetTableName(), indexes_);
  child_executor_->Init();
  schema_ = table_->GetSchema();
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row delete_row;
  RowId delete_rid;
  if(child_executor_->Next(&delete_row, &delete_rid)){
    heap_->ApplyDelete(delete_rid, exec_ctx_->GetTransaction());
    for(auto index_info :indexes_){
      Schema *schema = index_info->GetIndexKeySchema();
      vector<Field> del_entry;
      // 投影row
      for(auto column : schema->GetColumns()){
        uint32_t col_idx;
        schema_->GetColumnIndex(column->GetName(), col_idx);
        del_entry.push_back(*delete_row.GetField(col_idx));
      }
      // 删除entry
      Row del_key(del_entry);
      del_key.SetRowId(delete_rid);
      index_info->GetIndex()->RemoveEntry(del_key, delete_rid, exec_ctx_->GetTransaction());
    }
    return true;
  }else{
    return false;
  }
}