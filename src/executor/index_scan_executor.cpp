#include "executor/executors/index_scan_executor.h"
#include "planner/expressions/constant_value_expression.h"
#include "planner/expressions/logic_expression.h"
/**
 * TODO: Student Implement
 */
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  CatalogManager *catalog = exec_ctx_->GetCatalog();
  assert(catalog->GetTable(plan_->GetTableName(), table_) == DB_SUCCESS);
  indexes_ = plan_->indexes_;
  time_ = 0;
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  if(time_ != 0){
    if(time_ >= results_.size()){
      return false;
    }
    rid = &results_[time_];
    row->SetRowId(*rid);
    table_->GetTableHeap()->GetTuple(row, exec_ctx_->GetTransaction());
    time_++;
    return true;
  }
  auto cmp = plan_->filter_predicate_.get();
  if(cmp->GetType() == ExpressionType::ComparisonExpression){
    cmp = (ComparisonExpression *)plan_->filter_predicate_.get();
    GetResult((ComparisonExpression *)cmp, results_);
  }else if(cmp->GetType() == ExpressionType::LogicExpression){
    cmp = (LogicExpression *)plan_->filter_predicate_.get();
    for(auto cmp_child : cmp->GetChildren()){
      GetResult((ComparisonExpression *)cmp_child.get(), results_);
    }
  }
  if(results_.empty()) return false;
  rid = &results_[0];
  row->SetRowId(*rid);
  table_->GetTableHeap()->GetTuple(row, exec_ctx_->GetTransaction());

  time_++;
  return true;
}

void IndexScanExecutor::GetResult(ComparisonExpression *cmp_child, vector<RowId> &results)
{
  // 获取比较符号
  string cmp_operator = cmp_child->GetComparisonType();
  // 获取key，只考虑单列
  auto tmp = (ConstantValueExpression *)cmp_child->GetChildAt(1).get();
  vector<Field> f;
  f.push_back(tmp->val_);
  Row key(f);
  // 获取对应的index
  Index *use_index = nullptr;
  uint32_t col_id = ((ColumnValueExpression *)cmp_child->GetChildAt(0).get())->GetColIdx();
  for(auto index_info : indexes_){
    if(index_info->GetIndexKeySchema()->GetColumn(0)->GetTableInd() == col_id){
      use_index = index_info->GetIndex();
      break;
    }
  }
  vector<RowId> tmp_results;
  if(use_index == nullptr && plan_->need_filter_){
    for(auto iter = table_->GetTableHeap()->Begin(exec_ctx_->GetTransaction());
         iter != table_->GetTableHeap()->End(); iter++){
      if(cmp_child->Evaluate(&*iter).CompareEquals(Field(kTypeInt, 1)) == kTrue){
        tmp_results.push_back(iter->GetRowId());
      }
    }
  }else{
    use_index->ScanKey(key, tmp_results, exec_ctx_->GetTransaction(), cmp_operator);
  }
  results = intersection(tmp_results, results);
}

vector<RowId> IndexScanExecutor::intersection(const vector<RowId> &a, const vector<RowId> &b)
{
  vector<RowId> ret;
  if(a.empty()) ret = b;
  else if(b.empty()) ret = a;
  else{
    // 取交集
    for(auto ta : a){
      for(auto tb : b){
        if(ta == tb){
          ret.push_back(ta);
          break;
        }
      }
    }
  }
  return ret;
}