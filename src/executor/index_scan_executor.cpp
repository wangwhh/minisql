#include "executor/executors/index_scan_executor.h"
/**
* TODO: Student Implement
*/
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  CatalogManager *catalog = exec_ctx_->GetCatalog();
  TableInfo *table;
  assert(catalog->GetTable(plan_->GetTableName(), table) == DB_SUCCESS);
  heap_ = table->GetTableHeap();
  iter_ = heap_->Begin(exec_ctx_->GetTransaction());
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  iter_++;
  if(iter_ == heap_->End()){
    return false;
  }else{
    if(plan_->filter_predicate_ == nullptr){  // 无需筛选行
      if(plan_->need_filter_){  //需要筛选列
        for(auto index_info : plan_->indexes_){
          // TODO: ?
        }
      }else{    //无需筛选列
        *row = *iter_;
        *rid = (*iter_).GetRowId();
        return true;
      }

    }else{  //需要筛选行
      while( plan_->filter_predicate_->Evaluate(&*iter_).CompareEquals(Field(kTypeInt, 1)) != kTrue ){
        iter_++;
        if(iter_ == heap_->End()){
          return false;
        }
      }
      if(plan_->need_filter_){  //需要筛选列
        //TODO: ?
      }else{    //无需筛选列
        *row = *iter_;
        *rid = (*iter_).GetRowId();
        return true;
      }
    }
  }
}
