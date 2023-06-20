//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/plans/seq_scan_plan.h"
#include "planner/expressions/constant_value_expression.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_);
  heap_ = table_->GetTableHeap();
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  Row update_row;
  RowId update_rid;
  if(child_executor_->Next(&update_row, &update_rid)){
    update_row = GenerateUpdatedTuple(update_row);
  }else{
    return false;
  }
  heap_->UpdateTuple(update_row, update_rid, exec_ctx_->GetTransaction());

  return true;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
  vector<Field>update_fields;
  for(int i=0; i < src_row.GetFieldCount(); i++){
    update_fields.push_back(*src_row.GetField(i));
  }
  for(const auto& update_attr : plan_->update_attrs_){
    ConstantValueExpression *field_ptr = (ConstantValueExpression *)update_attr.second.get();
    Field copy(field_ptr->val_);
    update_fields[update_attr.first] = copy;
  }
  return Row(update_fields);
}