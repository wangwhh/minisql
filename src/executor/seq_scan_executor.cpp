//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan){}

void SeqScanExecutor::Init() {
  CatalogManager *catalog = exec_ctx_->GetCatalog();
  TableInfo *table;
  assert(catalog->GetTable(plan_->GetTableName(), table) == DB_SUCCESS);
  heap_ = table->GetTableHeap();
  iter_ = heap_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  // 先往后移一个
  iter_++;
  if(iter_ == heap_->End()){ //如果是结尾
    return false;
  }else{
    if(plan_->filter_predicate_ == nullptr){  // 没有where
      *row = *iter_;
      *rid = (*iter_).GetRowId();
      return true;
    }else{
      while( plan_->filter_predicate_->Evaluate(&*iter_).CompareEquals(Field(kTypeInt, 1)) != kTrue ){
        iter_++;
        if(iter_ == heap_->End()){
          return false;
        }
      }
      // 找到了
      *row = *iter_;
      *rid = (*iter_).GetRowId();
      return true;
    }
  }
}
