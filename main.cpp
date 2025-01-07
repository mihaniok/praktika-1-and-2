// server.cpp

#include <iostream>
#include <fstream>
#include <sys/stat.h>    
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdexcept>
#include <cstring>   
#include <thread>       
#include <sstream>
#include <string>
#include "json.hpp"

using namespace std;
using json = nlohmann::json;


// Односвязный список для строк (JSON) в таблице

struct TableDataNode {
    string record_str;       // хранит JSON-строку
    TableDataNode* next;

    TableDataNode(const string& s) : record_str(s), next(nullptr) {}
};


// Узел таблицы
struct Node {
    string name;            // имя таблицы
    TableDataNode* data;    // список JSON-строк
    Node* next;             // след. таблица

    Node(const string& n) : name(n), data(nullptr), next(nullptr) {}
};


// Структура базы данных
struct dbase {
    string schema_name;
    Node* head;
    int current_pk;

    dbase() : head(nullptr), current_pk(0) {}
    ~dbase() {
        // Удаляем список таблиц
        while (head) {
            Node* tmp = head;
            head = head->next;
            // Удаляем список data
            TableDataNode* d = tmp->data;
            while (d) {
                TableDataNode* dn = d->next;
                delete d;
                d = dn;
            }
            delete tmp;
        }
    }

    // Поиск таблицы
    Node* findNode(const string& table_name) {
        Node* cur = head;
        while (cur) {
            if (cur->name == table_name) return cur;
            cur = cur->next;
        }
        return nullptr;
    }

    // Добавить таблицу
    void addNode(const string& table_name) {
        Node* nd = new Node(table_name);
        nd->next = head;
        head = nd;
    }
};


// Добавление строки в таблицу (односвязный список)
void addDataToTable(Node* table_node, const string& json_str) {
    if (!table_node) return;
    TableDataNode* nd = new TableDataNode(json_str);
    nd->next = table_node->data;
    table_node->data = nd;
}


// Структура и массив условий WHERE

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


// mkdir (обёртка)
int my_mkdir(const char* path) {
    return mkdir(path, 0777);
}

// Создаём директории и CSV
void createDirectories(dbase& db, const json& structure) {
    if (my_mkdir(db.schema_name.c_str()) && errno != EEXIST) {
        cerr << "Failed to create directory: " << db.schema_name << endl;
    }
    for (auto it = structure.begin(); it != structure.end(); ++it) {
        string tname = it.key();
        string tpath = db.schema_name + "/" + tname;
        if (my_mkdir(tpath.c_str()) && errno != EEXIST) {
            cerr << "Failed to create directory: " << tpath << endl;
        }
        // Создаём 1.csv, если нет
        string fname = tpath + "/1.csv";
        ifstream ck(fname.c_str());
        if (!ck) {
            ofstream of(fname.c_str());
            if (of.is_open()) {
                auto& cols = it.value();
                for (size_t i=0; i<cols.size(); i++){
                    of << cols[i].get<string>();
                    if(i+1<cols.size()) of<<", ";
                }
                of<<"\n";
                of.close();
            }
        }
    }
}

void loadSchema(dbase& db, const string& schema_file) {
    ifstream f(schema_file.c_str());
    if(!f.is_open()){
        cerr<<"Failed to open schema file "<<schema_file<<"\n";
        return;
    }
    json j;
    f >> j;
    db.schema_name = j["name"];
    createDirectories(db, j["structure"]);
    for (auto it = j["structure"].begin(); it != j["structure"].end(); ++it){
        db.addNode(it.key());
    }
    cout<<"Schema loaded: "<<db.schema_name<<endl;
}


// Загрузка CSV

void loadData(dbase& db) {
    Node* cur = db.head;
    while (cur) {
        string path = db.schema_name + "/" + cur->name + "/1.csv";
        ifstream ifs(path.c_str());
        if (ifs.is_open()) {
            cout<<"Loading table: "<<cur->name<<endl;
            bool is_header=true;
            string line;
            while (getline(ifs,line)) {
                if(is_header) {
                    is_header=false;
                    continue;
                }
                // Разделим
                string fields[10];
                int count_fields=0;
                {
                    istringstream iss(line);
                    while(count_fields<10) {
                        string tmp;
                        if(!getline(iss,tmp,',')) break;
                        while(!tmp.empty() && (tmp.front()==' '||tmp.front()=='\t')) tmp.erase(tmp.begin());
                        while(!tmp.empty() && (tmp.back()==' '||tmp.back()=='\t'))   tmp.pop_back();
                        fields[count_fields++]= tmp;
                    }
                }
                // Собираем JSON
                json entry;
                if(count_fields>0) entry["name"]   = fields[0];
                if(count_fields>1) entry["age"]    = fields[1];
                if(count_fields>2) entry["adress"] = fields[2];
                if(count_fields>3) entry["number"] = fields[3];

                addDataToTable(cur, entry.dump());
                cout<<"Loaded entry: "<<entry.dump()<<endl;
            }
            ifs.close();
        }
        cur=cur->next;
    }
}


// Получить (примерное) кол-во столбцов из CSV-заголовка

int getColumnCount(dbase& db, const string& table) {
    Node* c = db.head;
    while(c){
        if(c->name==table){
            string path=db.schema_name+"/"+table+"/1.csv";
            ifstream f(path.c_str());
            if(f.is_open()){
                string hdr;
                if(getline(f,hdr)){
                    int ccount=0;
                    for(size_t i=0;i<hdr.size();i++){
                        if(hdr[i]==',') ccount++;
                    }
                    f.close();
                    return ccount+1;
                }
            }
        }
        c=c->next;
    }
    return 0;
}


// Сохранить 1 запись в CSV

void saveSingleEntryToCSV(dbase& db, const string& table, const json& entry) {
    string path = db.schema_name+"/"+table+"/1.csv";
    ofstream of(path.c_str(), ios::app);
    if(!of.is_open()){
        cerr<<"Failed to open "<<path<<endl;
        return;
    }
    if(entry.contains("name") && entry.contains("age")){
        of<<entry["name"].get<string>()<<", "
          <<entry["age"].get<string>()<<", ";
        if(entry.contains("adress")) {
            of<<entry["adress"].get<string>()<<", ";
        } else {
            of<<", ";
        }
        if(entry.contains("number")){
            of<<entry["number"].get<string>();
        }
        of<<"\n";
    }
    of.close();
}


// INSERT

void insertRecord(dbase& db, const string& table, json entry) {
    Node* tbl= db.findNode(table);
    if(!tbl){
        cerr<<"Table not found: "<<table<<endl;
        return;
    }
    entry["id"]= db.current_pk++;
    addDataToTable(tbl, entry.dump());
    saveSingleEntryToCSV(db, table, entry);
}


// DELETE

void deleteRow(dbase& db, const string& column, const string& value, const string& table) {
    Node* tbl= db.findNode(table);
    if(!tbl){
        cerr<<"Table not found "<<table<<endl;
        return;
    }
    // Упрощённая перепись списка
    TableDataNode dummy("");
    dummy.next= tbl->data;
    TableDataNode* prev=&dummy;
    TableDataNode* cur= tbl->data;
    bool found=false;
    while(cur){
        json e= json::parse(cur->record_str);
        bool match=false;
        if(e.contains(column)){
            if(e[column].get<string>()== value) match=true;
        }
        if(match){
            found=true;
            cout<<"Deleted row: "<< e.dump()<<endl;
            prev->next=cur->next;
            delete cur;
            cur=prev->next;
        } else {
            prev=cur;
            cur=cur->next;
        }
    }
    tbl->data=dummy.next;
    if(found){
        // Перезапись CSV
        string path=db.schema_name+"/"+table+"/1.csv";
        ofstream of(path.c_str());
        if(!of.is_open()){
            cerr<<"Failed to rewrite "<<path<<endl;
            return;
        }
        // Заголовок
        of<<"name, age, adress, number\n";
        TableDataNode* p=tbl->data;
        while(p){
            json e2=json::parse(p->record_str);
            if(e2.contains("name")&& e2.contains("age")){
                of<<e2["name"].get<string>()<<", "
                  <<e2["age"].get<string>()<<", "
                  <<(e2.contains("adress") ? e2["adress"].get<string>() : "")<<", "
                  <<(e2.contains("number") ? e2["number"].get<string>() : "")
                  <<"\n";
            }
            p=p->next;
        }
        of.close();
        cout<<"CSV rewritten: "<<path<<endl;
    } else {
        cout<<"Row with "<<column<<"="<<value<<" not found in "<<table<<endl;
    }
}



void parseWhereClause(const string& where_clause, ConditionList& cond_list, string& logical_op) {
    cond_list.count=0;
    logical_op.clear();

    size_t pos_and= where_clause.find(" AND ");
    size_t pos_or = where_clause.find(" OR ");
    if(pos_and!=string::npos){
        logical_op="AND";
    } else if(pos_or!=string::npos){
        logical_op="OR";
    }

    size_t start=0;
    while(start<where_clause.size()){
        size_t next_pos= string::npos;
        if(logical_op=="AND"){
            next_pos= where_clause.find(" AND ", start);
        } else if(logical_op=="OR"){
            next_pos= where_clause.find(" OR ", start);
        }
        string part;
        if(next_pos!=string::npos){
            part= where_clause.substr(start, next_pos-start);
            start= next_pos+5;
        } else {
            part= where_clause.substr(start);
            start= where_clause.size();
        }
        while(!part.empty() && (part.front()==' '||part.front()=='\t')) part.erase(part.begin());
        while(!part.empty() && (part.back()==' '||part.back()=='\t'))   part.pop_back();

        // ищем =, !=, <, <=, etc
        size_t p= part.find("!=");
        string op;
        if(p!=string::npos) op="!="; else {
            p=part.find('=');
            if(p!=string::npos) op="="; else {
                p=part.find('<');
                if(p!=string::npos){
                    if(p+1<part.size() && part[p+1]=='=') op="<="; else op="<";
                } else {
                    p=part.find('>');
                    if(p!=string::npos){
                        if(p+1<part.size() && part[p+1]=='=') op=">="; else op=">";
                    }
                }
            }
        }
        if(p==string::npos) continue;
        Condition c;
        c.op=op;
        string col= part.substr(0,p);
        string val= part.substr(p+op.size());
        while(!col.empty() && (col.front()==' '||col.front()=='\t')) col.erase(col.begin());
        while(!col.empty() && (col.back()==' '||col.back()=='\t'))   col.pop_back();
        while(!val.empty() && (val.front()==' '||val.front()=='\t'||val.front()=='\'')) val.erase(val.begin());
        while(!val.empty() && (val.back()==' '||val.back()=='\t'||val.back()=='\''))   val.pop_back();
        c.column= col;
        c.value= val;
        if(cond_list.count<MAX_COND){
            cond_list.conds[cond_list.count++]= c;
        }
    }
}

bool checkOneCondition(const json& entry, const Condition& c) {
    if(!entry.contains(c.column)) return false;
    string v= entry[c.column].get<string>();
    if(c.op=="=")   return (v==c.value);
    if(c.op=="!=")  return (v!=c.value);
    if(c.op=="<")   return (v< c.value);
    if(c.op==">")   return (v> c.value);
    if(c.op=="<=")  return (v<=c.value);
    if(c.op==">=")  return (v>=c.value);
    return false;
}

bool checkAllConditions(const json& entry, const ConditionList& cList, const string& logical_op) {
    if(cList.count==0) return true; // no conditions => pass
    if(logical_op=="AND"){
        for(int i=0;i<cList.count;i++){
            if(!checkOneCondition(entry, cList.conds[i])) return false;
        }
        return true;
    } else if(logical_op=="OR"){
        for(int i=0;i<cList.count;i++){
            if(checkOneCondition(entry, cList.conds[i])) return true;
        }
        return false;
    } else {
        // no operator => AND
        for(int i=0;i<cList.count;i++){
            if(!checkOneCondition(entry, cList.conds[i])) return false;
        }
        return true;
    }
}


void selectFromTable(dbase& db,
                     const string& table,
                     const string* columns,int col_count,
                     const ConditionList& cond_list,
                     const string& logical_op,
                     ostringstream& out)
{
    Node* tbl= db.findNode(table);
    if(!tbl){
        out<<"Table not found: "<<table<<"\n";
        return;
    }
    // Выводим "заголовок"
    for(int i=0;i<col_count;i++){
        if(i>0) out<<" ";
        out<<columns[i];
    }
    out<<"\n";

    bool data_found=false;
    TableDataNode* p= tbl->data;
    while(p){
        json e=json::parse(p->record_str);
        if(checkAllConditions(e, cond_list, logical_op)){
            data_found=true;
            // вывод
            for(int c=0;c<col_count;c++){
                if(c>0) out<<" ";
                if(columns[c]=="*"){
                    // выводим всё
                    bool first=true;
                    for(auto it=e.begin(); it!=e.end(); ++it){
                        if(!first) out<<" ";
                        out<< it.key()<<"="<< it.value().get<string>();
                        first=false;
                    }
                    break;
                } else {
                    if(e.contains(columns[c])){
                        out<< e[columns[c]].get<string>();
                    } else {
                        out<<"NULL";
                    }
                }
            }
            out<<"\n";
        }
        p=p->next;
    }
    if(!data_found){
        out<<"No data found in "<<table<<".\n";
    }
}


void selectFromMultipleTables(dbase& db,
                              const string* columns,int col_count,
                              const string* tables,int tab_count,
                              const ConditionList& cond_list,
                              const string& logical_op,
                              ostringstream& out)
{
    if(tab_count<=0){
        out<<"No tables specified.\n";
        return;
    }
    // Заголовок
    for(int i=0;i<col_count;i++){
        if(i>0) out<<" ";
        out<<columns[i];
    }
    out<<"\n";

    bool data_found=false;
    for(int t=0;t<tab_count;t++){
        Node* tbl= db.findNode(tables[t]);
        if(!tbl){
            out<<"Table not found: "<<tables[t]<<"\n";
            continue;
        }
        TableDataNode* p= tbl->data;
        while(p){
            json e=json::parse(p->record_str);
            if(checkAllConditions(e, cond_list, logical_op)){
                data_found=true;
                // вывод
                for(int c=0;c<col_count;c++){
                    if(c>0) out<<" ";
                    if(columns[c]=="*"){
                        bool first=true;
                        for(auto it=e.begin(); it!=e.end(); ++it){
                            if(!first) out<<" ";
                            out<<it.key()<<"="<< it.value().get<string>();
                            first=false;
                        }
                        break;
                    } else {
                        if(e.contains(columns[c])){
                            out<< e[columns[c]].get<string>();
                        } else {
                            out<<"NULL";
                        }
                    }
                }
                out<<"\n";
            }
            p=p->next;
        }
    }
    if(!data_found){
        out<<"No data found with conditions.\n";
    }
}

void crossJoinTables(dbase& db,
                     const string& table1,
                     const string& table2,
                     const string* columns,int col_count,
                     const ConditionList& cond_list,
                     const string& logical_op,
                     ostringstream& out)
{
    Node* t1= db.findNode(table1);
    Node* t2= db.findNode(table2);
    if(!t1){
        out<<"Table not found: "<<table1<<"\n";
        return;
    }
    if(!t2){
        out<<"Table not found: "<<table2<<"\n";
        return;
    }

    out << columns[0] <<" "<< columns[1] <<"\n";

    bool data_found=false;

    TableDataNode* p1= t1->data;
    while(p1){
        json e1= json::parse(p1->record_str);
        p1= p1->next;
        TableDataNode* p2= t2->data;
        while(p2){
            json e2= json::parse(p2->record_str);
            p2= p2->next;

            json comb= e1;
            for(auto it=e2.begin(); it!=e2.end(); ++it){
                if(comb.contains(it.key())){
                    comb["." + it.key()]= it.value();
                } else {
                    comb[it.key()] = it.value();
                }
            }
            // Проверим условия
            if(checkAllConditions(comb, cond_list, logical_op)){
                data_found=true;
                string nm= e1.contains("name") ? e1["name"].get<string>() : "NULL";
                string ag= e2.contains("age")  ? e2["age"].get<string>()  : "NULL";
                out<< nm <<" "<< ag <<"\n";
            }
        }
    }

    TableDataNode* q1= t2->data;
    while(q1){
        json e2= json::parse(q1->record_str);
        q1= q1->next;
        TableDataNode* q2= t1->data;
        while(q2){
            json e1= json::parse(q2->record_str);
            q2= q2->next;

            json comb= e2;
            for(auto it=e1.begin(); it!=e1.end(); ++it){
                if(comb.contains(it.key())){
                    comb["." + it.key()]= it.value();
                } else {
                    comb[it.key()]= it.value();
                }
            }
            if(checkAllConditions(comb, cond_list, logical_op)){
                data_found=true;
                // Вывод: (table2.name, table1.age)
                string nm= e2.contains("name") ? e2["name"].get<string>() : "NULL";
                string ag= e1.contains("age")  ? e1["age"].get<string>()  : "NULL";
                out<< nm <<" "<< ag <<"\n";
            }
        }
    }

    if(!data_found){
        out<<"No data found after CROSS JOIN.\n";
    }
}


// Обработка клиента

void handleClient(int client_socket, dbase& db) {
    char buf[4096];
    while(true){
        memset(buf,0,sizeof(buf));
        int n= read(client_socket, buf, sizeof(buf)-1);
        if(n<=0){
            cout<<"Client disconnected.\n";
            break;
        }
        string cmd(buf);
        while(!cmd.empty() && (cmd.back()=='\n'||cmd.back()=='\r')) cmd.pop_back();

        istringstream iss(cmd);
        string action;
        iss>>action;
        for(size_t i=0;i<action.size();i++){
            action[i]= toupper(action[i]);
        }

        if(action=="EXIT"){
            cout<<"Client requested EXIT.\n";
            break;
        }
        else if(action=="INSERT"){
            // INSERT <table> <name> <age> ...
            string table;
            iss>>table;
            // ...
            const int MAX_ARGS=10;
            string args[MAX_ARGS];
            int count_args=0;
            while(count_args<MAX_ARGS){
                if(!iss.good()) break;
                string tmp;
                if(!getline(iss,tmp,' ')) break;
                // remove quotes
                if(!tmp.empty() && tmp.front()=='"' && tmp.back()=='"'){
                    tmp= tmp.substr(1,tmp.size()-2);
                }
                args[count_args++]= tmp;
            }
            if(count_args<2){
                string e="Error: Not enough args for INSERT.\n";
                send(client_socket,e.c_str(),e.size(),0);
                continue;
            }
            json entry;
            entry["name"]= args[0];
            entry["age"] = args[1];
            if(count_args>2) entry["adress"]= args[2]; else entry["adress"]="";
            if(count_args>3) entry["number"]= args[3]; else entry["number"]="";
            insertRecord(db, table, entry);
            string ok="Data inserted.\n";
            send(client_socket, ok.c_str(), ok.size(),0);
        }
        else if(action=="DELETE"){
            // DELETE FROM <table> <col> <value>
            string from_word, table, column, value;
            iss>>from_word>>table>>column>>value;
            // uppercase from_word
            for(size_t i=0;i<from_word.size();i++){
                from_word[i]= toupper(from_word[i]);
            }
            if(from_word!="FROM"){
                string e="Error: invalid DELETE syntax.\n";
                send(client_socket,e.c_str(),e.size(),0);
                continue;
            }
            deleteRow(db, column, value, table);
            string ok="Row deleted.\n";
            send(client_socket, ok.c_str(), ok.size(),0);
        }
        else if(action=="SELECT"){
            // SELECT <columns> FROM ...
            size_t from_pos= cmd.find("FROM");
            if(from_pos==string::npos){
                string e="Error: SELECT must contain FROM.\n";
                send(client_socket,e.c_str(),e.size(),0);
                continue;
            }
            string select_part= cmd.substr(0, from_pos); // "SELECT name age "
            string from_part  = cmd.substr(from_pos);    // "FROM table1 CROSS JOIN table2..."

            const int MAX_COLS=10;
            string columns[MAX_COLS];
            int col_count=0;
            {
                size_t selp= select_part.find("SELECT");
                if(selp!=string::npos){
                    string cstr= select_part.substr(selp+6);
                    // trim
                    while(!cstr.empty() && (cstr.front()==' '||cstr.front()=='\t')) cstr.erase(cstr.begin());
                    while(!cstr.empty() && (cstr.back()==' '||cstr.back()=='\t'))   cstr.pop_back();
                    // Разбиваем по пробелам
                    istringstream ciss(cstr);
                    while(col_count<MAX_COLS){
                        string tmp;
                        if(!(ciss>>tmp)) break;
                        columns[col_count++]= tmp;
                    }
                }
            }

            // "FROM table1 CROSS JOIN table2 WHERE ..."
            istringstream fstm(from_part);
            string from_word;
            fstm >> from_word; // FROM
            string table1;
            fstm >> table1;    // table1

            bool is_cross=false;
            string maybe_cross;
            fstm >> maybe_cross; 
            string table2;
            if(maybe_cross=="CROSS"){
                // JOIN
                string join_word;
                fstm >> join_word; // JOIN
                if(join_word!="JOIN"){
                    string e="Error: invalid CROSS JOIN syntax.\n";
                    send(client_socket,e.c_str(), e.size(),0);
                    continue;
                }
                fstm >> table2;
                is_cross=true;
            } else {
            }
            // остаток (WHERE)
            string rest;
            getline(fstm, rest);
            ConditionList cond_list;
            cond_list.count=0;
            string logical_op;
            size_t wpos= rest.find("WHERE");
            if(wpos!=string::npos){
                string wcl= rest.substr(wpos+5);
                parseWhereClause(wcl, cond_list, logical_op);
            }
            ostringstream out;
            if(is_cross){
                crossJoinTables(db, table1, table2, columns, col_count,
                                cond_list, logical_op, out);
            } else {
                const int MAX_TABLES=5;
                string tabs[MAX_TABLES];
                int tab_count=0;
                if(!table1.empty()) {
                    tabs[tab_count++]= table1;
                }
                if(!maybe_cross.empty() && maybe_cross!="WHERE"){
                    tabs[tab_count++]= maybe_cross;
                }
                if(!table2.empty() && table2!="WHERE"){
                    tabs[tab_count++]= table2;
                }

                if(tab_count==1){
                    selectFromTable(db, tabs[0], columns, col_count, cond_list, logical_op, out);
                } else {
                    selectFromMultipleTables(db, columns, col_count, tabs, tab_count,
                                             cond_list, logical_op, out);
                }
            }

            string result= out.str();
            send(client_socket, result.c_str(), result.size(),0);
        }
        else {
            string e="Unknown command: "+cmd+"\n";
            send(client_socket, e.c_str(), e.size(),0);
        }
    }

    close(client_socket);
    cout<<"Connection closed.\n";
}


// main()

int main(){
    dbase db;
    loadSchema(db, "schema.json");
    loadData(db);

    int srv= socket(AF_INET, SOCK_STREAM, 0);
    if(srv<0){
        cerr<<"Can't create socket.\n";
        return 1;
    }
    struct sockaddr_in addr;
    addr.sin_family= AF_INET;
    addr.sin_addr.s_addr= INADDR_ANY;
    addr.sin_port= htons(7432);

    if(bind(srv,(struct sockaddr*)&addr,sizeof(addr))<0){
        cerr<<"Bind error.\n";
        close(srv);
        return 1;
    }
    listen(srv,5);
    cout<<"Server listening on port 7432...\n";

    while(true){
        int client_sock= accept(srv,nullptr,nullptr);
        if(client_sock<0){
            cerr<<"Accept error.\n";
            continue;
        }
        cout<<"Client connected.\n";
        thread t(handleClient, client_sock, ref(db));
        t.detach();
    }

    close(srv);
    return 0;
}
