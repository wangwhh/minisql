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
  for(auto index : index_info_){
    if(index->GetIndexName() == "unique"){
      unique_index_ = index->GetIndex();
      break;
    }
  }
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  vector<Field> fields;
  //vector<vector<Field>> index_keys(index_info_.size());
  ValuesPlanNode *values_plan_node = (ValuesPlanNode *)plan_->GetChildPlan().get();
  auto values = values_plan_node->values_;

  for(auto value : values){
    for(auto field_ptr : value){
      Field field = field_ptr->Evaluate(row);
      fields.push_back(field);
      /*// 遍历所有的index，看每个index有没有这个field
      for(int i=0; i < index_info_.size(); i++){
        auto index = index_info_[i];
        for(auto column : index->GetIndexKeySchema()->GetColumns()){
          // 如果有，把它加进去
          if(column->GetTableInd() == fields.size()){
            index_keys[i].push_back(field);
          }
        }
      }*/
    }
  }

  Row tuple(fields);

  // TODO: unique检测。更新index 啊啊啊完全乱了
  // 判断unique
  /*RowId result;
  if(unique_index_->InsertEntry() == DB_FAILED){
    cout << "Duplicate entry!" <<endl;
    return false;
  }*/
  if(table_heap_->InsertTuple(tuple, exec_ctx_->GetTransaction())){
    return false;
  }else{
    cout << "The tuple is too large." << endl;
  }
  return false;
}