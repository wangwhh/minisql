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
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row delete_row;
  RowId delete_rid;
  if(child_executor_->Next(&delete_row, &delete_rid)){
    heap_->ApplyDelete(delete_rid, exec_ctx_->GetTransaction());
    return true;
  }else{
    return false;
  }
}