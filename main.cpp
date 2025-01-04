#include <iostream>
#include <fstream>
#include <sys/stat.h>      
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdexcept>
#include <cstring>         
#include <sstream>
#include <string>
#include <thread>
#include "json.hpp"

using namespace std;
using json = nlohmann::json;


// Односвязный список для хранения записей в таблице
struct TableDataNode {
    string record_str;       // хранит JSON-строку
    TableDataNode* next;

    TableDataNode(const string& s) : record_str(s), next(nullptr) {}
};


// Узел, описывающий одну таблицу
struct Node {
    string name;            // имя таблицы
    TableDataNode* data;    // список записей (JSON)
    Node* next;             // указатель на следующую таблицу

    Node(const string& n) : name(n), data(nullptr), next(nullptr) {}
};


// Структура базы данных
struct dbase {
    string schema_name;
    Node* head;         // список таблиц
    int current_pk;     // счётчик PK

    dbase() : head(nullptr), current_pk(0) {}
    ~dbase() {
        // Удаляем список таблиц
        while (head) {
            Node* tmp = head;
            head = head->next;

            // Удаляем список записей
            TableDataNode* d = tmp->data;
            while (d) {
                TableDataNode* d_next = d->next;
                delete d;
                d = d_next;
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

    // Добавить таблицу (в начало списка)
    void addNode(const string& table_name) {
        Node* nd = new Node(table_name);
        nd->next = head;
        head = nd;
    }
};


// Функция для добавления JSON-строки в односвязный список таблицы
void addDataToTable(Node* table_node, const string& json_str) {
    if (!table_node) return;
    TableDataNode* nd = new TableDataNode(json_str);
    nd->next = table_node->data;
    table_node->data = nd;
}


// Структура для условий WHERE (без vector)
struct Condition {
    string column;
    string op;      // =, !=, <, >, <=, >=
    string value;
};


// Вспомогательные функции (mkdir), работа со схемой
int my_mkdir(const char* path) {
    return mkdir(path, 0777);
}

// Создаём директории и CSV-файлы
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
                // Пишем заголовок
                for (size_t i = 0; i < cols.size(); i++) {
                    of << cols[i].get<string>();
                    if (i + 1 < cols.size()) {
                        of << ", ";
                    }
                }
                of << "\n";
                of.close();
            }
        }
    }
}

void loadSchema(dbase& db, const string& schema_file) {
    ifstream f(schema_file.c_str());
    if (!f.is_open()) {
        cerr << "Failed to open schema file: " << schema_file << endl;
        return;
    }
    json j;
    f >> j;
    db.schema_name = j["name"];
    createDirectories(db, j["structure"]);
    for (auto it = j["structure"].begin(); it != j["structure"].end(); ++it) {
        db.addNode(it.key());
    }
    cout << "Schema loaded: " << db.schema_name << endl;
}


// Загрузка CSV-файлов
void loadData(dbase& db) {
    Node* cur = db.head;
    while (cur) {
        string path = db.schema_name + "/" + cur->name + "/1.csv";
        ifstream ifs(path.c_str());
        if (ifs.is_open()) {
            cout << "Loading table: " << cur->name << endl;
            bool is_header = true;
            string line;
            while (getline(ifs, line)) {
                if (is_header) {
                    is_header = false; // пропускаем заголовок
                    continue;
                }
                // Разбиваем
                string fields[10];
                int count_fields = 0;
                {
                    istringstream iss(line);
                    while (count_fields < 10) {
                        string tmp;
                        if (!getline(iss, tmp, ',')) break;
                        // trim
                        while (!tmp.empty() && (tmp.front() == ' ' || tmp.front() == '\t'))
                            tmp.erase(tmp.begin());
                        while (!tmp.empty() && (tmp.back() == ' ' || tmp.back() == '\t'))
                            tmp.pop_back();
                        fields[count_fields++] = tmp;
                    }
                }
                // собираем JSON
                json entry;
                if (count_fields > 0) entry["name"]   = fields[0];
                if (count_fields > 1) entry["age"]    = fields[1];
                if (count_fields > 2) entry["adress"] = fields[2];
                if (count_fields > 3) entry["number"] = fields[3];

                addDataToTable(cur, entry.dump());
                cout << "Loaded entry: " << entry.dump() << endl;
            }
            ifs.close();
        }
        cur = cur->next;
    }
}


// Функция для получения предполагаемого количества столбцов
int getColumnCount(dbase& db, const string& table) {
    // Ищем заголовок
    Node* p = db.head;
    while (p) {
        if (p->name == table) {
            string path = db.schema_name + "/" + table + "/1.csv";
            ifstream f(path.c_str());
            if (f.is_open()) {
                string hdr;
                if (getline(f, hdr)) {
                    int count_comma = 0;
                    for (size_t i = 0; i < hdr.size(); i++) {
                        if (hdr[i] == ',') count_comma++;
                    }
                    f.close();
                    return count_comma + 1;
                }
            }
        }
        p = p->next;
    }
    return 0;
}


// Сохранение одной записи в CSV
void saveSingleEntryToCSV(dbase& db, const string& table, const json& entry) {
    string path = db.schema_name + "/" + table + "/1.csv";
    ofstream of(path.c_str(), ios::app);
    if (!of.is_open()) {
        cerr << "Failed to open " << path << endl;
        return;
    }
    // Нужно наличие name, age
    if (entry.contains("name") && entry.contains("age")) {
        of << entry["name"].get<string>() << ", "
           << entry["age"].get<string>() << ", ";
        if (entry.contains("adress")) {
            of << entry["adress"].get<string>() << ", ";
        } else {
            of << ", ";
        }
        if (entry.contains("number")) {
            of << entry["number"].get<string>();
        }
        of << "\n";
    }
    of.close();
}


// Вставка (INSERT)
void insertRecord(dbase& db, const string& table, json entry) {
    Node* tbl = db.findNode(table);
    if (!tbl) {
        cerr << "Table not found: " << table << endl;
        return;
    }
    entry["id"] = db.current_pk++;
    // Добавляем в память
    addDataToTable(tbl, entry.dump());
    // Сохраняем в CSV
    saveSingleEntryToCSV(db, table, entry);
}


// Удаление (DELETE ...)
void deleteRow(dbase& db, const string& column, const string& value, const string& table) {
    Node* tbl = db.findNode(table);
    if (!tbl) {
        cerr << "Table not found: " << table << endl;
        return;
    }
    // Упрощённо — переписываем односвязный список
    TableDataNode dummy_head("");
    dummy_head.next = tbl->data;
    TableDataNode* prev = &dummy_head;
    TableDataNode* cur = tbl->data;
    bool found = false;
    while (cur) {
        json entry = json::parse(cur->record_str);
        bool match = false;
        if (entry.contains(column)) {
            if (entry[column].get<string>() == value) {
                match = true;
            }
        }
        if (match) {
            found = true;
            cout << "Deleted row: " << entry.dump() << endl;
            prev->next = cur->next;
            delete cur;
            cur = prev->next;
        } else {
            prev = cur;
            cur = cur->next;
        }
    }
    tbl->data = dummy_head.next;
    if (found) {
        // Перезаписываем CSV
        string path = db.schema_name + "/" + table + "/1.csv";
        ofstream of(path.c_str());
        if (!of.is_open()) {
            cerr << "Failed to rewrite " << path << endl;
            return;
        }
        // Заголовок
        of << "name, age, adress, number\n";
        // Перебираем
        TableDataNode* cur2 = tbl->data;
        while (cur2) {
            json e2 = json::parse(cur2->record_str);
            if (e2.contains("name") && e2.contains("age")) {
                of << e2["name"].get<string>() << ", "
                   << e2["age"].get<string>() << ", "
                   << (e2.contains("adress") ? e2["adress"].get<string>() : "") << ", "
                   << (e2.contains("number") ? e2["number"].get<string>() : "")
                   << "\n";
            }
            cur2 = cur2->next;
        }
        of.close();
        cout << "CSV file rewritten: " << path << endl;
    } else {
        cout << "Row with " << column << "=" << value << " not found in " << table << endl;
    }
}


// ConditionList — хранит массив условий (без vector)
const int MAX_COND = 100;

struct ConditionList {
    Condition conds[MAX_COND];
    int count;
    ConditionList() : count(0) {}
};


// Парсинг WHERE
void parseWhereClause(const string& where_clause, ConditionList& cond_list, string& logical_op) {
    cond_list.count = 0;
    logical_op.clear();

    size_t and_pos = where_clause.find(" AND ");
    size_t or_pos  = where_clause.find(" OR ");
    if (and_pos != string::npos) {
        logical_op = "AND";
    } else if (or_pos != string::npos) {
        logical_op = "OR";
    } else {
        logical_op = "";
    }

    size_t start = 0;
    while (start < where_clause.size()) {
        size_t next_pos = string::npos;
        if (logical_op == "AND") {
            next_pos = where_clause.find(" AND ", start);
        } else if (logical_op == "OR") {
            next_pos = where_clause.find(" OR ", start);
        }
        string part;
        if (next_pos != string::npos) {
            part = where_clause.substr(start, next_pos - start);
            start = next_pos + 5;
        } else {
            part = where_clause.substr(start);
            start = where_clause.size();
        }
        // trim
        while (!part.empty() && (part.front()==' '||part.front()=='\t')) part.erase(part.begin());
        while (!part.empty() && (part.back()==' '||part.back()=='\t'))   part.pop_back();

        // Ищем операторы
        size_t posn = part.find("!=");
        string op;
        if (posn != string::npos) {
            op = "!=";
        } else {
            posn = part.find('=');
            if (posn != string::npos) {
                op = "=";
            } else {
                posn = part.find('<');
                if (posn != string::npos) {
                    if (posn+1<part.size() && part[posn+1]=='=') {
                        op = "<=";
                    } else {
                        op = "<";
                    }
                } else {
                    posn = part.find('>');
                    if (posn != string::npos) {
                        if (posn+1<part.size() && part[posn+1]=='=') {
                            op = ">=";
                        } else {
                            op = ">";
                        }
                    }
                }
            }
        }
        if (posn == string::npos) {
            // некорректно
            continue;
        }
        Condition c;
        c.op = op;
        string col = part.substr(0, posn);
        string val = part.substr(posn + op.size());
        // trim
        while (!col.empty() && (col.front()==' '||col.front()=='\t')) col.erase(col.begin());
        while (!col.empty() && (col.back()==' '||col.back()=='\t'))   col.pop_back();
        while (!val.empty() && (val.front()==' '||val.front()=='\t'||val.front()=='\'')) val.erase(val.begin());
        while (!val.empty() && (val.back()==' '||val.back()=='\t'||val.back()=='\''))    val.pop_back();
        c.column = col;
        c.value  = val;
        if (cond_list.count < MAX_COND) {
            cond_list.conds[cond_list.count++] = c;
        }
    }
}

bool checkOneCondition(const json& entry, const Condition& c) {
    if (!entry.contains(c.column)) return false;
    string v = entry[c.column].get<string>();
    if (c.op == "=")   return (v == c.value);
    if (c.op == "!=")  return (v != c.value);
    if (c.op == "<")   return (v <  c.value);
    if (c.op == ">")   return (v >  c.value);
    if (c.op == "<=")  return (v <= c.value);
    if (c.op == ">=")  return (v >= c.value);
    return false;
}

bool checkAllConditions(const json& entry, const ConditionList& cond_list, const string& logical_op) {
    if (cond_list.count == 0) return true;
    if (logical_op == "AND") {
        for (int i = 0; i < cond_list.count; i++) {
            if (!checkOneCondition(entry, cond_list.conds[i])) return false;
        }
        return true;
    } else if (logical_op == "OR") {
        for (int i = 0; i < cond_list.count; i++) {
            if (checkOneCondition(entry, cond_list.conds[i])) return true;
        }
        return false;
    } else {
        // без оператора => AND
        for (int i = 0; i < cond_list.count; i++) {
            if (!checkOneCondition(entry, cond_list.conds[i])) return false;
        }
        return true;
    }
}


// SELECT без JOIN
void selectFromTable(dbase& db,
                     const string& table,
                     const string* columns,
                     int col_count,
                     const ConditionList& cond_list,
                     const string& logical_op,
                     ostringstream& result_stream)
{
    Node* tbl = db.findNode(table);
    if (!tbl) {
        result_stream << "Table not found: " << table << "\n";
        return;
    }

    // Заголовок
    for (int i = 0; i < col_count; i++) {
        if (i > 0) result_stream << ", ";
        result_stream << columns[i];
    }
    result_stream << "\n";

    bool data_found = false;
    TableDataNode* p = tbl->data;
    while (p) {
        json entry = json::parse(p->record_str);
        if (checkAllConditions(entry, cond_list, logical_op)) {
            data_found = true;
            // вывод полей
            for (int j = 0; j < col_count; j++) {
                if (j > 0) result_stream << ", ";
                // проверяем special case "*"
                if (columns[j] == "*") {
                    // вывести всё
                    bool first = true;
                    for (auto it = entry.begin(); it != entry.end(); ++it) {
                        if (!first) result_stream << ", ";
                        result_stream << it.key() << ": \"" << it.value().get<string>() << "\"";
                        first = false;
                    }
                    break;
                } else {
                    // обычная колонка
                    if (entry.contains(columns[j])) {
                        result_stream << entry[columns[j]].get<string>();
                    } else {
                        result_stream << "NULL";
                    }
                }
            }
            result_stream << ";\n";
        }
        p = p->next;
    }

    if (!data_found) {
        result_stream << "No data found in " << table << " with the specified conditions.\n";
    }
}


// SELECT из нескольких таблиц (UNION)
void selectFromMultipleTables(dbase& db,
                              const string* columns, int col_count,
                              const string* tables, int tab_count,
                              const ConditionList& cond_list,
                              const string& logical_op,
                              ostringstream& result_stream)
{
    if (tab_count <= 0) {
        result_stream << "No tables specified for SELECT.\n";
        return;
    }
    // Заголовок
    for (int i = 0; i < col_count; i++) {
        if (i > 0) result_stream << ", ";
        result_stream << columns[i];
    }
    result_stream << "\n";

    bool data_found = false;
    // Перебираем все таблицы
    for (int t = 0; t < tab_count; t++) {
        Node* tbl = db.findNode(tables[t]);
        if (!tbl) {
            result_stream << "Table not found: " << tables[t] << "\n";
            continue;
        }
        TableDataNode* p = tbl->data;
        while (p) {
            json entry = json::parse(p->record_str);
            if (checkAllConditions(entry, cond_list, logical_op)) {
                data_found = true;
                // Вывод
                for (int c = 0; c < col_count; c++) {
                    if (c > 0) result_stream << ", ";
                    if (columns[c] == "*") {
                        bool first = true;
                        for (auto it = entry.begin(); it != entry.end(); ++it) {
                            if (!first) result_stream << ", ";
                            result_stream << it.key() << ": \"" << it.value().get<string>() << "\"";
                            first = false;
                        }
                        break;
                    } else {
                        if (entry.contains(columns[c])) {
                            result_stream << entry[columns[c]].get<string>();
                        } else {
                            result_stream << "NULL";
                        }
                    }
                }
                result_stream << ";\n";
            }
            p = p->next;
        }
    }
    if (!data_found) {
        result_stream << "No data found in specified tables with the conditions.\n";
    }
}


// CROSS JOIN (с пробелом: table1 CROSS JOIN table2)
void crossJoinTables(dbase& db,
                     const string& table1,
                     const string& table2,
                     const string* columns, int col_count,
                     const ConditionList& cond_list,
                     const string& logical_op,
                     ostringstream& result_stream)
{
    Node* t1 = db.findNode(table1);
    Node* t2 = db.findNode(table2);
    if (!t1) {
        result_stream << "Table not found: " << table1 << "\n";
        return;
    }
    if (!t2) {
        result_stream << "Table not found: " << table2 << "\n";
        return;
    }

    // Проверка, спрашивают ли только "name"
    bool only_name = false;
    if (col_count == 1 && columns[0] == "name") {
        only_name = true;
        result_stream << "name1 name2\n";
    } else {
        // Вывод заголовка
        for (int i = 0; i < col_count; i++) {
            if (i > 0) result_stream << ", ";
            result_stream << columns[i];
        }
        result_stream << "\n";
    }

    bool data_found = false;
    // Декартово произведение
    for (TableDataNode* p1 = t1->data; p1; p1 = p1->next) {
        json j1 = json::parse(p1->record_str);
        for (TableDataNode* p2 = t2->data; p2; p2 = p2->next) {
            json j2 = json::parse(p2->record_str);
            // Комбинируем
            json comb = j1;
            // Если ключи совпадают, делаем .key
            for (auto it = j2.begin(); it != j2.end(); ++it) {
                if (comb.contains(it.key())) {
                    comb["." + it.key()] = it.value();
                } else {
                    comb[it.key()] = it.value();
                }
            }

            // WHERE
            // Преобразуем comb -> checkAllConditions
            if (checkAllConditions(comb, cond_list, logical_op)) {
                data_found = true;
                if (only_name) {
                    // Вывод: name1 name2
                    string n1 = j1.contains("name") ? j1["name"].get<string>() : "NULL";
                    string n2 = j2.contains("name") ? j2["name"].get<string>() : "NULL";
                    result_stream << n1 << " " << n2 << "\n";
                } else {
                    // Выводим col_count полей
                    for (int c = 0; c < col_count; c++) {
                        if (c > 0) result_stream << ", ";
                        if (columns[c] == "*") {
                            // Всё
                            bool first = true;
                            for (auto cc = comb.begin(); cc != comb.end(); ++cc) {
                                if (!first) result_stream << ", ";
                                result_stream << cc.key() << ": \"" << cc.value().get<string>() << "\"";
                                first = false;
                            }
                            break;
                        } else {
                            // Проверяем, не содержит ли . ?
                            size_t pos = columns[c].find('.');
                            if (pos != string::npos) {
                                // table1.name?
                                string col_name = columns[c].substr(pos+1);
                                if (comb.contains(col_name)) {
                                    result_stream << comb[col_name].get<string>();
                                } else {
                                    result_stream << "NULL";
                                }
                            } else {
                                // обычное поле
                                if (comb.contains(columns[c])) {
                                    result_stream << comb[columns[c]].get<string>();
                                } else {
                                    result_stream << "NULL";
                                }
                            }
                        }
                    }
                    result_stream << ";\n";
                }
            }
        }
    }

    if (!data_found) {
        result_stream << "No data found after CROSS JOIN.\n";
    }
}


// Обработка клиента (INSERT, SELECT, DELETE, CROSS JOIN ...)
void handleClient(int client_socket, dbase& db) {
    char buf[4096];
    while (true) {
        memset(buf, 0, sizeof(buf));
        int n = read(client_socket, buf, sizeof(buf)-1);
        if (n <= 0) {
            cout << "Client disconnected.\n";
            break;
        }
        string cmd(buf);
        // Удаляем \r\n
        while (!cmd.empty() && (cmd.back() == '\n' || cmd.back() == '\r')) {
            cmd.pop_back();
        }
        // парсим
        istringstream iss(cmd);
        string action;
        iss >> action;
        // Приводим к верхнему регистру
        for (size_t i = 0; i < action.size(); i++) {
            action[i] = toupper(action[i]);
        }

        if (action == "EXIT") {
            cout << "Client requested EXIT.\n";
            break;
        }
        else if (action == "INSERT") {
            // INSERT table ...
            string table;
            iss >> table;
            // Читаем все поля
            const int MAX_ARGS = 10;
            string args[MAX_ARGS];
            int count_args = 0;
            while (count_args < MAX_ARGS && iss >> ws) {
                string tmp;
                if (!getline(iss, tmp, ' ')) break;
                // Удаляем кавычки
                if (!tmp.empty() && tmp.front()=='"' && tmp.back()=='"') {
                    tmp = tmp.substr(1, tmp.size()-2);
                }
                args[count_args++] = tmp;
            }
            int col_need = getColumnCount(db, table);
            if (count_args > col_need) {
                string e = "Error: Too many arguments for INSERT.\n";
                send(client_socket, e.c_str(), e.size(), 0);
                continue;
            } else if (count_args < 2) {
                string e = "Error: Not enough arguments for INSERT.\n";
                send(client_socket, e.c_str(), e.size(), 0);
                continue;
            }

            json entry;
            entry["name"] = args[0];
            entry["age"]  = args[1];
            if (count_args > 2) {
                entry["adress"] = args[2];
            } else {
                entry["adress"] = "";
            }
            if (count_args > 3) {
                entry["number"] = args[3];
            } else {
                entry["number"] = "";
            }
            insertRecord(db, table, entry);
            string ok = "Data inserted.\n";
            send(client_socket, ok.c_str(), ok.size(), 0);
        }
        else if (action == "DELETE") {
            // DELETE FROM table col value
            string from_word, table, col, val;
            iss >> from_word >> table >> col >> val;
            // uppercase from_word
            for (size_t i=0; i<from_word.size(); i++) {
                from_word[i] = toupper(from_word[i]);
            }
            if (from_word != "FROM") {
                string e="Error: Invalid DELETE syntax.\n";
                send(client_socket, e.c_str(), e.size(),0);
                continue;
            }
            deleteRow(db, col, val, table);
            string ok="Row deleted.\n";
            send(client_socket, ok.c_str(), ok.size(), 0);
        }
        else if (action == "SELECT") {
            // SELECT ... FROM ...
            // Ищем "FROM"
            size_t pos_from = cmd.find("FROM");
            if (pos_from == string::npos) {
                string e="Error: SELECT must contain FROM.\n";
                send(client_socket, e.c_str(), e.size(),0);
                continue;
            }
            string select_part = cmd.substr(0, pos_from);
            string from_part   = cmd.substr(pos_from);

            // Парсим столбцы
            string col_str;
            {
                size_t sel_pos = select_part.find("SELECT");
                if (sel_pos != string::npos) {
                    col_str = select_part.substr(sel_pos+6);
                }
                // trim
                while (!col_str.empty() && (col_str.front()==' '||col_str.front()=='\t')) col_str.erase(col_str.begin());
                while (!col_str.empty() && (col_str.back()==' '||col_str.back()=='\t'))   col_str.pop_back();
            }
            // Разделяем по запятым
            const int MAX_COLS=10;
            string columns[MAX_COLS];
            int col_count=0;
            {
                istringstream iss2(col_str);
                while (col_count<MAX_COLS) {
                    string tmp;
                    if (!getline(iss2, tmp, ',')) break;
                    // trim
                    while (!tmp.empty() && (tmp.front()==' '||tmp.front()=='\t')) tmp.erase(tmp.begin());
                    while (!tmp.empty() && (tmp.back()==' '||tmp.back()=='\t'))   tmp.pop_back();
                    if (!tmp.empty()) {
                        columns[col_count++] = tmp;
                    }
                }
            }

            // from_part => "FROM table1 CROSS JOIN table2 ..."
            // Извлечём 'FROM'
            istringstream iss3(from_part);
            string from_word;
            iss3 >> from_word; // FROM
            string table1;
            iss3 >> table1;    // предполагается "table1" или "table1 CROSS"
            // Проверим CROSS JOIN
            bool is_cross = false;
            string next_word;
            iss3 >> next_word; // может быть CROSS, WHERE, или JOIN, или ничего
            string table2;
            if (next_word=="CROSS") {
                // затем JOIN
                string jn;
                iss3 >> jn; // JOIN
                if (jn!="JOIN") {
                    string e="Error: invalid CROSS JOIN syntax.\n";
                    send(client_socket,e.c_str(),e.size(),0);
                    continue;
                }
                iss3 >> table2; // table2
                is_cross = true;
            } else {
                // не CROSS => возвращаемся
                // (в идеале seekg, но можно manual)
                // Упростим: table2 = next_word (если есть)
                // ...
                table2=next_word; 
            }

            // Остаток
            string remaining;
            getline(iss3, remaining);

            // WHERE
            size_t pos_where = remaining.find("WHERE");
            string where_clause;
            bool has_where=false;
            if (pos_where!=string::npos) {
                has_where=true;
                where_clause=remaining.substr(pos_where+5);
            }

            ConditionList cList;
            string logical_op;
            if (has_where) {
                parseWhereClause(where_clause, cList, logical_op);
            }

            // Выполняем SELECT
            ostringstream out;
            if (is_cross) {
                // CROSS JOIN
                crossJoinTables(db, table1, table2,
                                columns, col_count,
                                cList, logical_op, out);
            } else {
                // Проверяем, может быть UNION
                // Разделим "table1 table2..."
                // Упростим, если next_word пуст => 1 таблица
                const int MAX_TABLES=5;
                string tabs[MAX_TABLES];
                int tab_count=0;
                // table1 уже есть
                if (!table1.empty()) {
                    tabs[tab_count++] = table1;
                }
                if (!table2.empty() && table2!="WHERE") {
                    // может быть leftover
                    // trim
                    while (!table2.empty() && (table2.front()==' '||table2.front()=='\t')) table2.erase(table2.begin());
                    while (!table2.empty() && (table2.back()==' '||table2.back()=='\t'))   table2.pop_back();
                    if (!table2.empty()) {
                        tabs[tab_count++] = table2;
                    }
                }
                if (tab_count==1) {
                    selectFromTable(db, tabs[0], columns, col_count, cList, logical_op, out);
                } else {
                    // UNION
                    selectFromMultipleTables(db, columns, col_count, tabs, tab_count, cList, logical_op, out);
                }
            }

            string res = out.str();
            send(client_socket, res.c_str(), res.size(), 0);
        }
        else {
            string unknown="Unknown command: "+cmd+"\n";
            send(client_socket, unknown.c_str(), unknown.size(), 0);
        }
    }
    // Закрытие
    close(client_socket);
    cout<<"Connection closed.\n";
}

int main() {
    dbase db;
    loadSchema(db, "schema.json");  // Создаём директории, добавляем таблицы
    loadData(db);                   // Загружаем CSV

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv<0) {
        cerr<<"Can't create socket.\n";
        return 1;
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(7432);

    if (bind(srv,(struct sockaddr*)&addr,sizeof(addr))<0) {
        cerr<<"Bind error.\n";
        close(srv);
        return 1;
    }
    listen(srv, 5);
    cout<<"Server listening on 7432...\n";

    while (true) {
        int client_sock = accept(srv,nullptr,nullptr);
        if (client_sock<0) {
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
