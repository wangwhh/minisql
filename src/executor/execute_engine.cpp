#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** After you finish the code for the CatalogManager section,
   *  you can uncomment the commented code.
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
   **/
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if(!current_db_.empty())
    context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  __clock_t start_time, end_time;
  start_time = clock();
  string db_name = ast->child_->val_;
  DBStorageEngine *db = new DBStorageEngine(db_name, true);
  if(dbs_.find(db_name) != dbs_.end()){
    cout << "Can't create database '" << db_name << "';" ;
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(pair<string, DBStorageEngine *>(db_name, db));
  end_time = clock();
  cout << "Successfully create database '" << db_name << "' in" << (double)(end_time-start_time)/CLOCKS_PER_SEC << "sec." << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  __clock_t start_time, end_time;
  start_time = clock();
  string db_name = ast->child_->val_;

  if(dbs_.find(db_name) == dbs_.end()){
    cout << "Can't drop database '" << db_name << "'; ";
    return DB_NOT_EXIST;
  }
  if(db_name == current_db_){
    current_db_ = "";
  }
  DBStorageEngine *db = dbs_[db_name];
  delete db;  // 析构
  dbs_.erase(db_name);
  end_time = clock();
  cout << "Successfully drop database" << db_name << "in" << (double)(end_time - start_time)/CLOCKS_PER_SEC << "sec." << endl;
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  clock_t start_time, end_time;
  start_time = clock();
  cout << "*====================*" << endl;
  cout << "|  Database          |" << endl;
  cout << "+--------------------+" << endl;
  for(const auto& it : dbs_){
    cout << "|  " << setw(18) << left << it.first << "|" << endl;
  }
  cout << "+====================+" << endl;
  end_time = clock();
  cout << dbs_.size() << " rows in set (" << (double)(end_time - start_time) / CLOCKS_PER_SEC << " sec)" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if(dbs_.find(db_name) == dbs_.end()){
    cout << "Unknown database '" << db_name << "'; " << endl;
    return DB_NOT_EXIST;
  }
  current_db_ = db_name;
  cout << "Database changed. Current database: '" << db_name << "'. "<<endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  clock_t start_time, end_time;
  start_time = clock(); //计时开始
  if(current_db_.empty())
  {
    cout << "You haven't chosen a database!" << endl;
    return DB_FAILED;
  }
  cout << "+====================+" << endl;
  cout << "| Tables in " << setw(8) << left << current_db_ << " |" << endl;
  cout << "+--------------------+" << endl;
  std::vector<TableInfo*> tables;
  dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for(auto it : tables)
    cout << "| " << setw(18) << left << it->GetTableName() << " |" << endl;
  cout << "+====================+" << endl;
  end_time = clock();
  cout << tables.size() <<" rows in set (" << (double)(end_time - start_time) / CLOCKS_PER_SEC << " sec)" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  clock_t start_time, end_time;
  if(current_db_.empty()){
    cout << "You haven't chosen a database!" << endl;
    return DB_FAILED;
  }
  start_time = clock();
  string table_name = ast->child_->val_;
  auto ptr = ast->child_->next_;
  vector<Column *>columns;
  vector<string> primary_keys;

  while(ptr != nullptr){
    if(ptr->type_ == kNodeColumnDefinitionList){
      int i = 1;  // 表内index
      auto col_def_ptr = ptr->child_;
      while(col_def_ptr != nullptr){
        string col_name, col_type;
        int char_num;
        bool is_unique = false;
        TypeId type;
        if(col_def_ptr->val_ != nullptr){
          if(strcmp(col_def_ptr->val_, "unique") == 0){
            is_unique = true;
          }else if(strcmp(col_def_ptr->val_, "primary keys") == 0){  // 主键
            auto primary_key_ptr = col_def_ptr->child_;
            while(primary_key_ptr != nullptr){
              string key_name = primary_key_ptr->val_;
              primary_keys.push_back(key_name);
              primary_key_ptr = primary_key_ptr->next_;
            }
            col_def_ptr = col_def_ptr->next_;
            continue;
          }
        }

        col_name = col_def_ptr->child_->val_;  // 列名
        col_type = col_def_ptr->child_->next_->val_; // 列的类型
        if(col_type == "char"){
          type = kTypeChar;
          char_num =atoi(col_def_ptr->child_->next_->child_->val_);
          if(char_num < 0 || strchr(col_def_ptr->child_->next_->child_->val_, '.') != nullptr){
            cout << "Invalid input!" << endl;
            return DB_FAILED;
          }
        }else if(col_type == "int"){
          type = kTypeInt;
        }else if(col_type == "float"){
          type = kTypeFloat;
        }else{
          cout << "Invalid input!" << endl;
          return DB_FAILED;
        }

        // 这里index是主键才有吗？
        Column *col;
        if(type == kTypeChar){
          col = new Column(col_name, type, char_num, i++, false, is_unique);
        }else{
          col = new Column(col_name, type, i++, false, is_unique);
        }
        columns.push_back(col);
        col_def_ptr = col_def_ptr->next_;
      }
    }
    ptr = ptr->next_;
  }
  TableSchema *schema = new TableSchema(columns, true); // is_manage是啥，直接写成true了
  TableInfo *table_info;
  IndexInfo *index_info;
  auto ret = context->GetCatalog()->CreateTable(table_name, schema, context->GetTransaction(), table_info);
  // 为primary创建索引
  if(ret == DB_SUCCESS && !primary_keys.empty()){
    context->GetCatalog()->CreateIndex(table_name, "primary", primary_keys, context->GetTransaction(), index_info, "bptree");
  }
  end_time = clock();
  if(ret == DB_SUCCESS)
    cout << "Successfully create table '" << table_name << "' in " << (double)(end_time - start_time)/CLOCKS_PER_SEC << "sec." << endl;
  return ret;
}

dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  clock_t start_time, end_time;
  start_time = clock(); //计时开始
  string table_name = ast->child_->val_;
  if(current_db_.empty())
  {
    cout << "You haven't chosen a database!" << endl;
    return DB_FAILED;
  }
  TableInfo *table_info = nullptr;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) == DB_TABLE_NOT_EXIST)
  {
    cout << "Invalid table!" << endl;
    return DB_TABLE_NOT_EXIST;
  }
  dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
  end_time = clock();
  cout << "Successfully drop table '" << table_name << "' in " << (double)(end_time - start_time) / CLOCKS_PER_SEC << "sec." << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  clock_t start_time, end_time;
  int row_cnt = 0;
  start_time = clock(); //计时开始
  if(current_db_.empty())
  {
    cout << "You haven't chosen a database!" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if(context->GetCatalog()->GetTables(tables) == DB_FAILED){
    return DB_FAILED;
  }
  cout << "+=========+===========+==========+==============+==============+==========+============+" << endl;
  cout << "| Table   | Is_unique | Key_name | Seq_in_index | Column_name  | Nullable | Index_type |" << endl;
  cout << "+---------+-----------+----------+--------------+--------------+----------+------------+" << endl;
  for(auto table: tables){  // 遍历该数据库的所有表
    string table_name = table->GetTableName();

    vector<IndexInfo *> indexes;
    context->GetCatalog()->GetTableIndexes(table_name, indexes);
    for(auto index: indexes){ // 遍历表中的所有索引
      string key_name = index->GetIndexName();  // 索引名称
      string index_type = "bptree";
      Schema *schema = index->GetIndexKeySchema();  // 索引的表
      vector<Column *> columns = schema->GetColumns();
      int seq_in_index = 1;
      for(auto column :columns){  // 遍历索引中所有column
        bool is_unique = column->IsUnique();
        string col_name = column->GetName();
        bool is_null = column->IsNullable();
        cout << "| " << setw(8) << left << table_name
             << "| " << setw(10) << right << is_unique
             << "| " << setw(9) << left << key_name
             << "| " << setw(13) << right << seq_in_index++
             << "| " << setw(13) << left << col_name
             << "| " << setw(9) << right << is_null
             << "| " << setw(11) << left << index_type
             << "|"  << endl;
        row_cnt++;
      }
    }
  }
  cout << "+=========+===========+==========+==============+==============+==========+============+" << endl;
  end_time = clock();
  cout << row_cnt <<" rows in set (" << (double)(end_time - start_time) / CLOCKS_PER_SEC << " sec)" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  clock_t start_time, end_time;
  start_time = clock(); //计时开始
  if(current_db_.empty())
  {
    cout << "You haven't chosen a database!" << endl;
    return DB_FAILED;
  }
  string index_name = ast->child_->val_;
  string table_name = ast->child_->next_->val_;
  vector<string> index_keys;

  auto index_keys_ptr = ast->child_->next_->next_->child_;
  while(index_keys_ptr != nullptr){
    string index_key_name = index_keys_ptr->val_;
    index_keys.push_back(index_key_name);
    index_keys_ptr = index_keys_ptr->next_;
  }

  auto index_type_ptr = ast->child_->next_->next_->next_;
  string index_type = "bptree";
  if(index_type_ptr != nullptr){
    index_type = index_type_ptr->child_->val_;
  }

  IndexInfo *index_info;
  auto ret = context->GetCatalog()->CreateIndex(table_name, index_name, index_keys, context->GetTransaction(), index_info, index_type);
  if(ret == DB_TABLE_NOT_EXIST){
    cout << "Invalid table name!" << endl;
  }else if(ret == DB_COLUMN_NAME_NOT_EXIST){
    cout << "Invalid column name!" << endl;
  }else if(ret == DB_INDEX_ALREADY_EXIST){
    cout << "Index '" << index_name << "' already exists!" << endl;
  }else if(ret == DB_SUCCESS){
    end_time = clock();
    cout << "Successfully create index '" << index_name << "' in " << (double)(end_time - start_time)/CLOCKS_PER_SEC << "sec." <<endl;
  }
  return ret;
}

/**
 * 框架有问题？
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  clock_t start_time, end_time;
  start_time = clock(); //计时开始
  if(current_db_.empty())
  {
    cout << "You haven't chosen a database!" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if(context->GetCatalog()->GetTables(tables) == DB_FAILED){
    return DB_FAILED;
  }

  string index_name = ast->child_->val_;
  int drop_cnt = 0;
  for(auto table : tables){
    string table_name = table->GetTableName();
    if(context->GetCatalog()->DropIndex(table_name, index_name) == DB_SUCCESS){
      cout << "Drop index '" << index_name << "' on table '" << table_name << "';"<< endl;
      drop_cnt++;
    }
  }
  end_time = clock();
  cout << "Successfully drop " << drop_cnt << "indexes in " << (double)(end_time-start_time)/CLOCKS_PER_SEC << "sec." << endl;
  return DB_FAILED;
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  ASSERT(ast->type_ == kNodeQuit, "Unexpected node type.");
  return DB_QUIT;
}
