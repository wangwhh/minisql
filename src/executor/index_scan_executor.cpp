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
  indexes_ = plan_->indexes_;
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  vector<RowId> results;
  auto cmp = (ComparisonExpression *)plan_->filter_predicate_.get();
  for(auto index_info : indexes_){
    auto index = index_info->GetIndex();

    index->ScanKey(*row, results, exec_ctx_->GetTransaction(), cmp->GetComparisonType());

  }
}
