// server.cpp

#include <iostream>
#include <fstream>
#include <sys/stat.h>       // mkdir
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdexcept>
#include <cstring>          // memset
#include <thread>          
#include <sstream>
#include <string>
#include <vector>
#include "json.hpp"

using namespace std;
using json = nlohmann::json;


// Односвязный список для хранения JSON-строк таблицы

struct TableDataNode {
    string record_str;
    TableDataNode* next;
    TableDataNode(const string& s) : record_str(s), next(nullptr) {}
};


// Узел, описывающий одну таблицу

struct Node {
    string name;            // имя таблицы
    TableDataNode* data;    // список JSON-строк
    Node* next;             // следующий узел (таблица)

    Node(const string& n) : name(n), data(nullptr), next(nullptr) {}
};


// Структура базы данных

struct dbase {
    string schema_name;
    Node* head;

    dbase() : head(nullptr) {}
    ~dbase() {
        // Удаляем список таблиц
        while(head){
            Node* tmp= head;
            head= head->next;
            // Удаляем список data
            TableDataNode* d= tmp->data;
            while(d){
                TableDataNode* dn= d->next;
                delete d;
                d= dn;
            }
            delete tmp;
        }
    }

    // Поиск таблицы
    Node* findNode(const string& table_name){
        Node* cur= head;
        while(cur){
            if(cur->name == table_name) return cur;
            cur= cur->next;
        }
        return nullptr;
    }

    // Добавить таблицу (в начало списка)
    void addNode(const string& table_name){
        Node* nd= new Node(table_name);
        nd->next= head;
        head= nd;
    }
};


// Добавление строки JSON в таблицу

void addDataToTable(Node* table_node, const string& json_str){
    if(!table_node) return;
    TableDataNode* nd= new TableDataNode(json_str);
    nd->next= table_node->data;
    table_node->data= nd;
}


// Структура и список условий WHERE

struct Condition {
    string column;
    string op;      // =, !=, <, >, <=, >=
    string value;
};

const int MAX_COND = 100;
struct ConditionList {
    Condition conds[MAX_COND];
    int count;
    ConditionList() : count(0) {}
};


// mkdir-обёртка

int my_mkdir(const char* path){
    return mkdir(path, 0777);
}

// Создаём директории и CSV-файлы по схеме
void createDirectories(dbase& db, const json& structure) {
    if(my_mkdir(db.schema_name.c_str()) && errno != EEXIST){
        cerr << "Failed to create directory: " << db.schema_name << endl;
    }
    for(auto it = structure.begin(); it != structure.end(); ++it){
        string tname = it.key();
        string tpath = db.schema_name + "/" + tname;
        if(my_mkdir(tpath.c_str()) && errno != EEXIST){
            cerr << "Failed to create directory: " << tpath << endl;
        }
        string fname = tpath + "/1.csv";
        ifstream ck(fname.c_str());
        if(!ck){
            ofstream of(fname.c_str());
            if(of.is_open()){
                auto& cols = it.value();
                // Пишем заголовок
                for(size_t i = 0; i < cols.size(); i++){
                    of << cols[i].get<string>();
                    if(i + 1 < cols.size()) of << " ";
                }
                of << "\n";
                of.close();
            }
        }
    }
}

// Загрузка схемы
void loadSchema(dbase& db, const string& schema_file){
    ifstream f(schema_file.c_str());
    if(!f.is_open()){
        cerr << "Failed to open schema file: " << schema_file << "\n";
        return;
    }
    json j;
    f >> j;
    db.schema_name = j["name"];
    createDirectories(db, j["structure"]);
    for(auto it = j["structure"].begin(); it != j["structure"].end(); ++it){
        db.addNode(it.key());
    }
    cout << "Schema loaded: " << db.schema_name << endl;
}


// Загрузка CSV

void loadData(dbase& db){
    Node* cur = db.head;
    while(cur){
        string path = db.schema_name + "/" + cur->name + "/1.csv";
        ifstream ifs(path.c_str());
        if(ifs.is_open()){
            cout << "Loading table: " << cur->name << endl;
            bool is_header = true;
            string line;
            while(getline(ifs, line)){
                if(is_header){
                    is_header = false;
                    continue;
                }
                // поля
                string fields[10];
                int count_fields = 0;
                {
                    istringstream iss(line);
                    while(count_fields < 10){
                        string tmp;
                        if(!getline(iss, tmp, ' ')) break; 
                        // trim
                        while(!tmp.empty() && (tmp.front() == ' ' || tmp.front() == '\t')) tmp.erase(tmp.begin());
                        while(!tmp.empty() && (tmp.back() == ' ' || tmp.back() == '\t'))   tmp.pop_back();
                        fields[count_fields++] = tmp;
                    }
                }
                json entry;
                if(count_fields > 0) entry["name"]   = fields[0];
                if(count_fields > 1) entry["age"]    = fields[1];
                if(count_fields > 2) entry["adress"] = fields[2];
                if(count_fields > 3) entry["number"] = fields[3];

                addDataToTable(cur, entry.dump());
                cout << "Loaded entry: " << entry.dump() << endl;
            }
            ifs.close();
        }
        cur = cur->next;
    }
}


// Сохранение одной записи в CSV

void saveSingleEntryToCSV(dbase& db, const string& table, const json& entry){
    string path = db.schema_name + "/" + table + "/1.csv";
    ofstream of(path.c_str(), ios::app);
    if(!of.is_open()){
        cerr << "Failed to open " << path << endl;
        return;
    }
    if(entry.contains("name") && entry.contains("age")){
        of << entry["name"].get<string>() << " "
           << entry["age"].get<string>() << " ";
        if(entry.contains("adress")){
            of << entry["adress"].get<string>() << " ";
        }
        else{
            of << "NULL ";
        }
        if(entry.contains("number")){
            of << entry["number"].get<string>();
        }
        else{
            of << "NULL";
        }
        of << "\n";
    }
    of.close();
}


// INSERT

void insertRecord(dbase& db, const string& table, json entry){
    Node* tbl = db.findNode(table);
    if(!tbl){
        cerr << "Table not found: " << table << endl;
        return;
    }
    addDataToTable(tbl, entry.dump());
    saveSingleEntryToCSV(db, table, entry);
}


// DELETE

void deleteRow(dbase& db, const string& column, const string& value, const string& table){
    Node* tbl = db.findNode(table);
    if(!tbl){
        cerr << "Table not found " << table << endl;
        return;
    }
    TableDataNode dummy("");
    dummy.next = tbl->data;
    TableDataNode* prev = &dummy;
    TableDataNode* cur = tbl->data;
    bool found = false;
    while(cur){
        json e = json::parse(cur->record_str);
        bool match = false;
        if(e.contains(column)){
            if(e[column].get<string>() == value) match = true;
        }
        if(match){
            found = true;
            cout << "Deleted row: " << e.dump() << endl;
            prev->next = cur->next;
            delete cur;
            cur = prev->next;
        }
        else{
            prev = cur;
            cur = cur->next;
        }
    }
    tbl->data = dummy.next;
    if(found){
        // Перезаписываем CSV
        string path = db.schema_name + "/" + table + "/1.csv";
        ofstream of(path.c_str());
        if(!of.is_open()){
            cerr << "Failed to rewrite " << path << endl;
            return;
        }
        // Заголовок
        of << "name age adress number\n";
        TableDataNode* p = tbl->data;
        while(p){
            json e2 = json::parse(p->record_str);
            if(e2.contains("name") && e2.contains("age")){
                of << e2["name"].get<string>() << " "
                   << e2["age"].get<string>() << " "
                   << (e2.contains("adress") ? e2["adress"].get<string>() : "NULL") << " "
                   << (e2.contains("number") ? e2["number"].get<string>() : "NULL") << "\n";
            }
            p = p->next;
        }
        of.close();
        cout << "CSV file rewritten: " << path << endl;
    }
    else{
        cout << "Row with " << column << "=" << value << " not found in " << table << endl;
    }
}


// Парсинг WHERE

void parseWhereClause(const string& where_clause, ConditionList& cond_list, string& logical_op){
    cond_list.count = 0;
    logical_op.clear();
    size_t pos_and = where_clause.find(" AND ");
    size_t pos_or = where_clause.find(" OR ");
    if(pos_and != string::npos){
        logical_op = "AND";
    }
    else if(pos_or != string::npos){
        logical_op = "OR";
    }
    size_t start = 0;
    while(start < where_clause.size()){
        size_t next_pos = string::npos;
        if(logical_op == "AND"){
            next_pos = where_clause.find(" AND ", start);
        }
        else if(logical_op == "OR"){
            next_pos = where_clause.find(" OR ", start);
        }
        string part;
        if(next_pos != string::npos){
            part = where_clause.substr(start, next_pos - start);
            start = next_pos + 5;
        }
        else{
            part = where_clause.substr(start);
            start = where_clause.size();
        }
        while(!part.empty() && (part.front() == ' ' || part.front() == '\t')) part.erase(part.begin());
        while(!part.empty() && (part.back() == ' ' || part.back() == '\t'))   part.pop_back();

        size_t p = part.find("!=");
        string op;
        if(p != string::npos) op = "!=";
        else{
            p = part.find('=');
            if(p != string::npos) op = "=";
            else{
                p = part.find('<');
                if(p != string::npos){
                    if(p + 1 < part.size() && part[p+1] == '=') { op = "<="; p++; }
                    else op = "<";
                }
                else{
                    p = part.find('>');
                    if(p != string::npos){
                        if(p + 1 < part.size() && part[p+1] == '=') { op = ">="; p++; }
                        else op = ">";
                    }
                }
            }
        }

        if(p == string::npos) continue;
        Condition c;
        c.op = op;
        string col = part.substr(0, p);
        string val = part.substr(p + op.size());
        while(!col.empty() && (col.front() == ' ' || col.front() == '\t')) col.erase(col.begin());
        while(!col.empty() && (col.back() == ' ' || col.back() == '\t'))   col.pop_back();
        while(!val.empty() && (val.front() == ' ' || val.front() == '\t' || val.front() == '\'')) val.erase(val.begin());
        while(!val.empty() && (val.back() == ' ' || val.back() == '\t' || val.back() == '\''))   val.pop_back();
        c.column = col;
        c.value = val;
        if(cond_list.count < MAX_COND){
            cond_list.conds[cond_list.count++] = c;
        }
    }
}

bool checkOneCondition(const json& entry, const Condition& c){
    if(!entry.contains(c.column)) return false;
    string v = entry[c.column].get<string>();
    if(c.op == "=")   return (v == c.value);
    if(c.op == "!=")  return (v != c.value);
    if(c.op == "<")   return (v < c.value);
    if(c.op == ">")   return (v > c.value);
    if(c.op == "<=")  return (v <= c.value);
    if(c.op == ">=")  return (v >= c.value);
    return false;
}

bool checkAllConditions(const json& entry, const ConditionList& clist, const string& logical_op){
    if(clist.count == 0) return true;
    if(logical_op == "AND"){
        for(int i = 0; i < clist.count; i++){
            if(!checkOneCondition(entry, clist.conds[i])) return false;
        }
        return true;
    }
    else if(logical_op == "OR"){
        for(int i = 0; i < clist.count; i++){
            if(checkOneCondition(entry, clist.conds[i])) return true;
        }
        return false;
    }
    else{
        // нет логического оператора => AND
        for(int i = 0; i < clist.count; i++){
            if(!checkOneCondition(entry, clist.conds[i])) return false;
        }
        return true;
    }
}


// SELECT (одна таблица)

void selectFromTable(dbase& db,
                     const string& table,
                     const string* columns, int col_count,
                     const ConditionList& cond_list,
                     const string& logical_op,
                     ostringstream& out)
{
    Node* tbl = db.findNode(table);
    if(!tbl){
        out << "Table not found: " << table << "\n";
        return;
    }
    // Заголовок
    for(int i = 0; i < col_count; i++){
        if(i > 0) out << " ";
        out << columns[i];
    }
    out << "\n";

    bool data_found = false;
    TableDataNode* p = tbl->data;
    while(p){
        json e = json::parse(p->record_str);
        if(checkAllConditions(e, cond_list, logical_op)){
            data_found = true;
            for(int c = 0; c < col_count; c++){
                if(c > 0) out << " ";
                if(columns[c] == "*"){
                    bool first = true;
                    for(auto it = e.begin(); it != e.end(); ++it){
                        if(!first) out << " ";
                        out << it.key() << "=" << it.value().get<string>();
                        first = false;
                    }
                    break;
                }
                else{
                    if(e.contains(columns[c])){
                        out << e[columns[c]].get<string>();
                    }
                    else{
                        out << "NULL";
                    }
                }
            }
            out << "\n";
        }
        p = p->next;
    }
    if(!data_found){
        out << "No data found in " << table << ".\n";
    }
}


// SELECT (несколько таблиц)

void selectFromMultipleTables(dbase& db,
                              const string* columns, int col_count,
                              const string* tables, int tab_count,
                              const ConditionList& cond_list,
                              const string& logical_op,
                              ostringstream& out)
{
    if(tab_count <= 0){
        out << "No tables specified.\n";
        return;
    }
    // Заголовок (предполагается, что столбцы одинаковы во всех таблицах)
    for(int i = 0; i < col_count; i++){
        if(i > 0) out << " ";
        out << columns[i];
    }
    out << "\n";

    bool data_found = false;
    for(int t = 0; t < tab_count; t++){
        Node* tbl = db.findNode(tables[t]);
        if(!tbl){
            out << "Table not found: " << tables[t] << "\n";
            continue;
        }
        TableDataNode* p = tbl->data;
        while(p){
            json e = json::parse(p->record_str);
            if(checkAllConditions(e, cond_list, logical_op)){
                data_found = true;
                for(int c = 0; c < col_count; c++){
                    if(c > 0) out << " ";
                    if(columns[c] == "*"){
                        bool first = true;
                        for(auto it = e.begin(); it != e.end(); ++it){
                            if(!first) out << " ";
                            out << it.key() << "=" << it.value().get<string>();
                            first = false;
                        }
                        break;
                    }
                    else{
                        if(e.contains(columns[c])){
                            out << e[columns[c]].get<string>();
                        }
                        else{
                            out << "NULL";
                        }
                    }
                }
                out << "\n";
            }
            p = p->next;
        }
    }
    if(!data_found){
        out << "No data found in the specified tables.\n";
    }
}


// CROSS JOIN 

void crossJoinTables(dbase& db,
                     const string& table1,
                     const string& table2,
                     const string* columns, int col_count,
                     const ConditionList& cond_list,
                     const string& logical_op,
                     ostringstream& out)
{
    Node* t1 = db.findNode(table1);
    Node* t2 = db.findNode(table2);
    if(!t1){
        out << "Table not found: " << table1 << "\n";
        return;
    }
    if(!t2){
        out << "Table not found: " << table2 << "\n";
        return;
    }

    // Выводим «заголовок» (просто перечислим columns)
    for(int i = 0; i < col_count; i++){
        if(i > 0) out << " ";
        out << columns[i];
    }
    out << "\n";

    bool data_found = false;

    // Для каждой пары (row1, row2) из (table1 × table2) делаем ДВА прохода:
    // pass=1 => столбцы с чётным индексом берем из table1, с нечётным => из table2
    // pass=2 => наоборот
    TableDataNode* p1 = t1->data;
    while(p1){
        json e1 = json::parse(p1->record_str);
        TableDataNode* p2 = t2->data;
        while(p2){
            json e2 = json::parse(p2->record_str);

            // Проход №1
            {
                // Построим временный JSON comb1
                json comb1;
                for(int c = 0; c < col_count; c++){
                    // если c чётный => берем e1[columns[c]], иначе e2[columns[c]]
                    // (чётный — c%2==0)
                    if((c % 2) == 0){
                        // берем из e1
                        if(e1.contains(columns[c])){
                            comb1[ columns[c] ] = e1[ columns[c] ].get<string>();
                        } else {
                            comb1[ columns[c] ] = "NULL";
                        }
                    } else {
                        // берем из e2
                        if(e2.contains(columns[c])){
                            comb1[ columns[c] ] = e2[ columns[c] ].get<string>();
                        } else {
                            comb1[ columns[c] ] = "NULL";
                        }
                    }
                }
                // Проверяем условия
                if(checkAllConditions(comb1, cond_list, logical_op)){
                    data_found = true;
                    // Вывод
                    for(int c = 0; c < col_count; c++){
                        if(c > 0) out << " ";
                        out << comb1[ columns[c] ].get<string>();
                    }
                    out << "\n";
                }
            }

            // Проход №2
            {
                json comb2;
                for(int c = 0; c < col_count; c++){
                    // теперь наоборот: чётный => table2, нечётный => table1
                    if((c % 2) == 0){
                        // берем из e2
                        if(e2.contains(columns[c])){
                            comb2[ columns[c] ] = e2[ columns[c] ].get<string>();
                        } else {
                            comb2[ columns[c] ] = "NULL";
                        }
                    } else {
                        // берем из e1
                        if(e1.contains(columns[c])){
                            comb2[ columns[c] ] = e1[ columns[c] ].get<string>();
                        } else {
                            comb2[ columns[c] ] = "NULL";
                        }
                    }
                }
                if(checkAllConditions(comb2, cond_list, logical_op)){
                    data_found = true;
                    for(int c = 0; c < col_count; c++){
                        if(c > 0) out << " ";
                        out << comb2[ columns[c] ].get<string>();
                    }
                    out << "\n";
                }
            }

            p2 = p2->next;
        }
        p1 = p1->next;
    }

    if(!data_found){
        out << "No data found after CROSS JOIN.\n";
    }
}


// Обработка клиента

void handleClient(int client_socket, dbase& db) {
    char buf[4096];
    while(true){
        memset(buf, 0, sizeof(buf));
        int n = read(client_socket, buf, sizeof(buf)-1);
        if(n <= 0){
            cout << "Client disconnected.\n";
            break;
        }
        string cmd(buf);
        while(!cmd.empty() && (cmd.back() == '\n' || cmd.back() == '\r')){
            cmd.pop_back();
        }
        istringstream iss(cmd);
        string action;
        iss >> action;
        for(size_t i = 0; i < action.size(); i++){
            action[i] = toupper(action[i]);
        }

        if(action == "EXIT"){
            cout << "Client requested EXIT.\n";
            break;
        }
        else if(action == "INSERT"){
            // INSERT <table> <name> <age> [<adress>] [<number>]
            string table;
            iss >> table;
            const int MAX_ARGS = 10;
            vector<string> args;
            string tmp;
            while(iss >> tmp && args.size() < MAX_ARGS){
                // remove quotes
                if(!tmp.empty() && tmp.front() == '"' && tmp.back() == '"'){
                    tmp = tmp.substr(1, tmp.size()-2);
                }
                args.push_back(tmp);
            }
            if(args.size() < 2){
                string e = "Error: Not enough args for INSERT.\n";
                send(client_socket, e.c_str(), e.size(), 0);
                continue;
            }
            json entry;
            entry["name"] = args[0];
            entry["age"] = args[1];
            if(args.size() > 2) entry["adress"] = args[2]; else entry["adress"] = "";
            if(args.size() > 3) entry["number"] = args[3]; else entry["number"] = "";
            cout << "INSERT command: table=" << table << ", name=" << entry["name"] 
                 << ", age=" << entry["age"] << ", adress=" << entry["adress"] 
                 << ", number=" << entry["number"] << endl;
            insertRecord(db, table, entry);
            string ok = "Data inserted.\n";
            send(client_socket, ok.c_str(), ok.size(), 0);
        }
        else if(action == "DELETE"){
            // DELETE FROM <table> <column> <value>
            string from_word, table, col, val;
            iss >> from_word >> table >> col >> val;
            for(size_t i = 0; i < from_word.size(); i++){
                from_word[i] = toupper(from_word[i]);
            }
            if(from_word != "FROM"){
                string e = "Error: invalid DELETE syntax.\n";
                send(client_socket, e.c_str(), e.size(), 0);
                continue;
            }
            // Отладочное сообщение
            cout << "DELETE command: table=" << table << ", column=" << col 
                 << ", value=" << val << endl;
            deleteRow(db, col, val, table);
            string ok = "Row deleted.\n";
            send(client_socket, ok.c_str(), ok.size(), 0);
        }
        else if(action == "SELECT"){
            // SELECT <columns> FROM <tables> [CROSS JOIN <table>] [WHERE ...]
            // Определяем, содержит ли запрос CROSS JOIN
            size_t cross_pos = cmd.find("CROSS JOIN");
            bool is_cross = false;
            string table1, table2;
            string columns_str, tables_str, where_str;
            ConditionList cond_list;
            string logical_op;

            if(cross_pos != string::npos){
                is_cross = true;
                // Разделяем строку на части
                // SELECT <columns> FROM <table1> CROSS JOIN <table2> [WHERE ...]
                size_t select_pos = cmd.find("SELECT");
                size_t from_pos = cmd.find("FROM");
                size_t where_pos = cmd.find("WHERE");

                if(select_pos == string::npos || from_pos == string::npos){
                    string e = "Error: Invalid SELECT syntax.\n";
                    send(client_socket, e.c_str(), e.size(), 0);
                    continue;
                }

                columns_str = cmd.substr(select_pos + 6, from_pos - (select_pos +6));
                size_t first = columns_str.find_first_not_of(" \t");
                size_t last = columns_str.find_last_not_of(" \t");
                if(first != string::npos && last != string::npos){
                    columns_str = columns_str.substr(first, last - first +1);
                }

                // Извлекаем таблицы
                // FROM <table1> CROSS JOIN <table2> [WHERE ...]
                size_t cross_join_pos = cmd.find("CROSS JOIN");
                tables_str = cmd.substr(from_pos +4, cross_join_pos - (from_pos +4));
                // Удаляем пробелы
                first = tables_str.find_first_not_of(" \t");
                size_t end_cross = tables_str.find_last_not_of(" \t");
                if(first != string::npos && end_cross != string::npos){
                    tables_str = tables_str.substr(first, end_cross - first +1);
                }
                table1 = tables_str;
                size_t table2_start = cross_join_pos + 10; // длина "CROSS JOIN"
                size_t where_start = cmd.find("WHERE", table2_start);
                if(where_start != string::npos){
                    table2 = cmd.substr(table2_start, where_start - table2_start);
                    where_str = cmd.substr(where_start +5);
                }
                else{
                    table2 = cmd.substr(table2_start);
                }
                // Удаляем пробелы
                first = table2.find_first_not_of(" \t");
                size_t l = table2.find_last_not_of(" \t");
                if(first != string::npos && l != string::npos){
                    table2 = table2.substr(first, l - first +1);
                }

                // Парсим условия WHERE, если есть
                if(where_pos != string::npos){
                    parseWhereClause(where_str, cond_list, logical_op);
                }

                // Парсим колонки (разделение по пробелам)
                const int MAX_COLS = 10;
                string columns[MAX_COLS];
                int col_count = 0;
                istringstream ciss(columns_str);
                while(col_count < MAX_COLS && ciss >> columns[col_count]){
                    col_count++;
                }

                // Выполняем CROSS JOIN
                ostringstream out;
                crossJoinTables(db, table1, table2, columns, col_count, cond_list, logical_op, out);
                string result = out.str();
                send(client_socket, result.c_str(), result.size(), 0);
            }
            else{
                // Обработка обычного SELECT (одна или несколько таблиц без CROSS JOIN)
                // SELECT <columns> FROM <tables> [WHERE ...]
                size_t select_pos = cmd.find("SELECT");
                size_t from_pos = cmd.find("FROM");
                size_t where_pos = cmd.find("WHERE");

                if(select_pos == string::npos || from_pos == string::npos){
                    string e = "Error: Invalid SELECT syntax.\n";
                    send(client_socket, e.c_str(), e.size(), 0);
                    continue;
                }

                columns_str = cmd.substr(select_pos +6, from_pos - (select_pos +6));
                // Удаляем пробелы
                size_t first = columns_str.find_first_not_of(" \t");
                size_t last = columns_str.find_last_not_of(" \t");
                if(first != string::npos && last != string::npos){
                    columns_str = columns_str.substr(first, last - first +1);
                }

                // Извлекаем таблицы
                // FROM <tables> [WHERE ...]
                string tables_and_where;
                if(where_pos != string::npos){
                    tables_and_where = cmd.substr(from_pos +4, where_pos - (from_pos +4));
                    where_str = cmd.substr(where_pos +5);
                }
                else{
                    tables_and_where = cmd.substr(from_pos +4);
                }
                // Удаляем пробелы
                first = tables_and_where.find_first_not_of(" \t");
                size_t end_where = tables_and_where.find_last_not_of(" \t");
                if(first != string::npos && end_where != string::npos){
                    tables_and_where = tables_and_where.substr(first, end_where - first +1);
                }

                // Извлекаем таблицы (разделенные пробелами)
                vector<string> tables;
                istringstream tiss(tables_and_where);
                string tbl;
                while(tiss >> tbl){
                    tables.push_back(tbl);
                }

                // Парсим условия WHERE, если есть
                if(where_pos != string::npos){
                    parseWhereClause(where_str, cond_list, logical_op);
                }

                // Парсим колонки (разделение по пробелам)
                const int MAX_COLS = 10;
                string columns[MAX_COLS];
                int col_count = 0;
                istringstream ciss(columns_str);
                while(col_count < MAX_COLS && ciss >> columns[col_count]){
                    col_count++;
                }

                // Выполняем SELECT
                ostringstream out;
                if(tables.size() == 1){
                    selectFromTable(db, tables[0], columns, col_count, cond_list, logical_op, out);
                }
                else{
                    // Поддержка нескольких таблиц (UNION)
                    const int MAX_TABLES = 5;
                    string tbls[MAX_TABLES];
                    int tab_count = 0;
                    for(auto &t: tables){
                        if(tab_count < MAX_TABLES){
                            tbls[tab_count++] = t;
                        }
                    }
                    selectFromMultipleTables(db, columns, col_count, tbls, tab_count, cond_list, logical_op, out);
                }
                string result = out.str();
                send(client_socket, result.c_str(), result.size(), 0);
            }
        }
        else{
            string e = "Unknown command: " + cmd + "\n";
            send(client_socket, e.c_str(), e.size(), 0);
        }
    }

    close(client_socket);
    cout << "Connection closed.\n";
}


// main()

int main(){
    dbase db;
    loadSchema(db, "schema.json");
    loadData(db);

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if(srv < 0){
        cerr << "Can't create socket.\n";
        return 1;
    }
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(7432);

    if(bind(srv, (struct sockaddr*)&addr, sizeof(addr)) < 0){
        cerr << "Bind error. Port might be in use.\n";
        close(srv);
        return 1;
    }
    listen(srv, 5);
    cout << "Server listening on port 7432...\n";

    while(true){
        int client_sock = accept(srv, nullptr, nullptr);
        if(client_sock < 0){
            cerr << "Accept error.\n";
            continue;
        }
        cout << "Client connected.\n";
        thread t(handleClient, client_sock, ref(db));
        t.detach();
    }

    close(srv);
    return 0;
}
