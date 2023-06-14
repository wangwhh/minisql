//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  CatalogManager *catalog = exec_ctx_->GetCatalog();
  TableInfo *table;
  assert(catalog->GetTable(plan_->GetTableName(), table) == DB_SUCCESS);
  heap_ = table->GetTableHeap();
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  child_executor_->Next(row, rid);
  //TODO: 错误检测？什么叫更新index？
  if(heap_->InsertTuple(*row, exec_ctx_->GetTransaction())){
    return true;
  }
  return false;
}