// server.cpp

#include <iostream>
#include <fstream>
#include <sys/stat.h> // Для mkdir
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <stdexcept>
#include <cstring> // Для memset
#include "json.hpp" // Библиотека для работы с JSON (скачайте json.hpp из https://github.com/nlohmann/json)
#include <sstream> // Для istringstream
#include <vector>
#include <algorithm> // Для std::remove_if и std::transform

using namespace std;
using json = nlohmann::json;

// ---------------------------------------------------------------------------
// Структура для представления таблицы
// ---------------------------------------------------------------------------
struct Node {
    string name;
    vector<string> data; // Хранение данных в формате JSON-строк
    Node* next;

    Node(const string& name) : name(name), next(nullptr) {}
};

// ---------------------------------------------------------------------------
// Структура для базы данных
// ---------------------------------------------------------------------------
struct dbase {
    string schema_name;
    Node* head;
    int current_pk;

    dbase() : head(nullptr), current_pk(0) {}

    ~dbase() {
        while (head) {
            Node* temp = head;
            head = head->next;
            delete temp;
        }
    }

    Node* findNode(const string& table_name) {
        Node* current = head;
        while (current) {
            if (current->name == table_name) {
                return current;
            }
            current = current->next;
        }
        return nullptr;
    }

    void addNode(const string& table_name) {
        Node* new_node = new Node(table_name);
        new_node->next = head;
        head = new_node;
    }

    size_t getColumnCount(const string& table) {
        Node* table_node = findNode(table);
        if (table_node) {
            string filename = schema_name + "/" + table + "/1.csv"; 
            ifstream file(filename);
            if (file) {
                string header;
                if (getline(file, header)) {
                    size_t comma_count = 0;
                    for(char c : header){
                        if(c == ',') comma_count++;
                    }
                    return comma_count + 1; // Количество колонок = количество запятых + 1
                }
            }
        }
        return 0; 
    }

    void load() {
        Node* current = head;
        while (current) {
            try {
                string filename = schema_name + "/" + current->name + "/1.csv"; 
                cout << "Loading table: " << current->name << " from file: " << filename << endl; // Отладка
                ifstream file(filename);
                if (file) {
                    string line;
                    bool is_header = true;
                    while (getline(file, line)) {
                        // Удаление начальных и конечных пробелов
                        size_t start = line.find_first_not_of(" \t");
                        size_t end = line.find_last_not_of(" \t");
                        if(start != string::npos && end != string::npos){
                            line = line.substr(start, end - start + 1);
                        }

                        if (is_header) {
                            is_header = false;
                            continue;
                        }

                        istringstream iss(line);
                        vector<string> fields;
                        string field;

                        while (getline(iss, field, ',')) {
                            // Удаление пробелов
                            size_t f_start = field.find_first_not_of(" \t");
                            size_t f_end = field.find_last_not_of(" \t");
                            if(f_start != string::npos && f_end != string::npos){
                                field = field.substr(f_start, f_end - f_start + 1);
                            }
                            if (!field.empty()) {
                                fields.push_back(field);
                            }
                        }

                        if (fields.size() >= 1) { // Минимум поле 'name'
                            json entry;
                            entry["name"] = fields[0];
                            if (fields.size() > 1) entry["age"] = fields[1];
                            if (fields.size() > 2) entry["adress"] = fields[2];
                            if (fields.size() > 3) entry["number"] = fields[3];

                            current->data.push_back(entry.dump());
                            cout << "Loaded entry: " << entry.dump() << endl; // Отладка
                        } 
                    }
                } else {
                    throw runtime_error("Failed to open data file: " + filename);
                }
            } catch (const exception& e) {
                cout << "Error: " << e.what() << endl;
            }
            current = current->next;
        }
    }
};

// ---------------------------------------------------------------------------
// Структура для условий WHERE
// ---------------------------------------------------------------------------
struct Condition {
    string column;
    string op;   // Например, =, <, >, != и т.д.
    string value;
};

// ---------------------------------------------------------------------------
// Функция для создания директорий согласно схеме
// ---------------------------------------------------------------------------
void createDirectories(dbase& db, const json& structure) {
    try {
        if (mkdir(db.schema_name.c_str(), 0777) && errno != EEXIST) {
            throw runtime_error("Failed to create directory: " + db.schema_name);
        }

        for (auto it = structure.begin(); it != structure.end(); ++it) {
            string table_name = it.key();
            string table_path = db.schema_name + "/" + table_name;

            if (mkdir(table_path.c_str(), 0777) && errno != EEXIST) {
                throw runtime_error("Failed to create directory: " + table_path);
            }
            string filename = table_path + "/1.csv";

            ifstream check_file(filename);
            if (!check_file) {
                ofstream file(filename);
                if (file.is_open()) {
                    auto& columns = it.value();
                    string header = "";
                    for (size_t i = 0; i < columns.size(); ++i) {
                        header += columns[i].get<string>();
                        if (i < columns.size() - 1) header += ", ";
                    }
                    file << header << "\n";
                    file.close();
                    cout << "Created file with header: " << filename << endl; // Отладка
                }
            }
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

// ---------------------------------------------------------------------------
// Функция загрузки схемы из файла
// ---------------------------------------------------------------------------
void loadSchema(dbase& db, const string& schema_file) {
    try {
        ifstream file(schema_file);
        if (file) {
            json schema;
            file >> schema;
            db.schema_name = schema["name"];
            createDirectories(db, schema["structure"]);
            for (auto it = schema["structure"].begin(); it != schema["structure"].end(); ++it) {
                db.addNode(it.key());
            }
            cout << "Schema loaded successfully." << endl; // Отладка
        } else {
            throw runtime_error("Failed to open schema file.");
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

// ---------------------------------------------------------------------------
// Функция для сохранения одной записи в CSV
// ---------------------------------------------------------------------------
void saveSingleEntryToCSV(dbase& db, const string& table, const json& entry) {
    try {
        string filename = db.schema_name + "/" + table + "/1.csv"; 
        ofstream file(filename, ios::app);
        if (file) {
            if (entry.contains("name") && entry.contains("age")) {
                file << entry["name"].get<string>() << ", "
                     << entry["age"].get<string>() << ", "
                     << entry["adress"].get<string>() << ", "
                     << entry["number"].get<string>();
                file << "\n"; 
                cout << "Data successfully saved for: " << entry.dump() << endl; // Отладка
            } else {
                throw runtime_error("Entry must contain 'name' and 'age'.");
            }
        } else {
            throw runtime_error("Failed to open data file for saving: " + filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

// ---------------------------------------------------------------------------
// Функция для вставки записи (INSERT)
// ---------------------------------------------------------------------------
void insertRecord(dbase& db, const string& table, json entry) {
    Node* table_node = db.findNode(table);
    if (table_node) {
        entry["id"] = to_string(db.current_pk); 
        db.current_pk++; // Увеличение первичного ключа

        table_node->data.push_back(entry.dump());

        saveSingleEntryToCSV(db, table, entry);
    } else {
        cout << "Table not found: " << table << endl;
    }
}

// ---------------------------------------------------------------------------
// Функция для удаления записи (DELETE)
// ---------------------------------------------------------------------------
void deleteRow(dbase& db, const string& column, const string& value, const string& table) {
    Node* table_node = db.findNode(table);
    if (table_node) {
        vector<string> new_data;
        bool found = false;

        for (const auto& record_str : table_node->data) {
            json entry = json::parse(record_str);
            if (entry.contains(column) && entry[column].get<string>() == value) {
                found = true;
                cout << "Deleted row: " << entry.dump() << endl; // Отладка
            } else {
                new_data.push_back(record_str);
            }
        }

        if (found) {
            table_node->data = new_data;
            // Перезапись CSV файла
            try {
                string filename = db.schema_name + "/" + table + "/1.csv"; 
                ofstream file(filename); 

                if (file) {
                    // Запись заголовка
                    file << "name, age, adress, number\n";

                    for (const auto& record_str : table_node->data) {
                        json entry = json::parse(record_str);
                        file << entry["name"].get<string>() << ", "
                             << entry["age"].get<string>() << ", "
                             << entry["adress"].get<string>() << ", "
                             << entry["number"].get<string>() << "\n";
                    }
                    file.close();
                    cout << "CSV file rewritten successfully: " << filename << endl; // Отладка
                } else {
                    throw runtime_error("Failed to open data file for rewriting: " + filename);
                }
            } catch (const exception& e) {
                cout << "Error: " << e.what() << endl;
            }
        } else {
            cout << "Row with " << column << " = " << value << " not found in " << table << endl;
        }
    } else {
        cout << "Table not found: " << table << endl;
    }
}

// ---------------------------------------------------------------------------
// Функции для парсинга и проверки условий WHERE
// ---------------------------------------------------------------------------
struct Condition; // уже объявлена выше

void parseWhereClause(const string& where_clause, vector<Condition>& conditions, string& logical_op) {
    // Определение логического оператора между условиями (AND/OR)
    size_t and_pos = where_clause.find(" AND ");
    size_t or_pos = where_clause.find(" OR ");
    if (and_pos != string::npos) {
        logical_op = "AND";
    } else if (or_pos != string::npos) {
        logical_op = "OR";
    } else {
        logical_op = ""; // Нет логического оператора или только одно условие
    }

    size_t start = 0;
    while (start < where_clause.length()) {
        size_t end = string::npos;
        if (logical_op == "AND") {
            end = where_clause.find(" AND ", start);
        } else if (logical_op == "OR") {
            end = where_clause.find(" OR ", start);
        }

        string condition_str;
        if (end != string::npos) {
            condition_str = where_clause.substr(start, end - start);
            start = end + 5; // Пропускаем " AND " или " OR "
        }
        else {
            condition_str = where_clause.substr(start);
            start = where_clause.length();
        }

        // Парсим отдельное условие
        size_t pos = condition_str.find("!=");
        string op;
        if (pos != string::npos) {
            op = "!=";
        } else {
            pos = condition_str.find('=');
            if (pos != string::npos) {
                op = "=";
            } else {
                pos = condition_str.find('<');
                if (pos != string::npos) {
                    if (condition_str[pos + 1] == '=') {
                        op = "<=";
                        pos++;
                    } else {
                        op = "<";
                    }
                } else {
                    pos = condition_str.find('>');
                    if (pos != string::npos) {
                        if (condition_str[pos + 1] == '=') {
                            op = ">=";
                            pos++;
                        } else {
                            op = ">";
                        }
                    }
                }
            }
        }

        if (pos == string::npos) {
            // Невалидный формат условия
            continue;
        }

        string column = condition_str.substr(0, pos);
        string value = condition_str.substr(pos + op.length());

        // Удаление пробелов и кавычек
        size_t col_start = column.find_first_not_of(" \t");
        size_t col_end = column.find_last_not_of(" \t");
        if(col_start != string::npos && col_end != string::npos){
            column = column.substr(col_start, col_end - col_start + 1);
        }

        size_t val_start = value.find_first_not_of(" \t'");
        size_t val_end = value.find_last_not_of(" \t'");
        if(val_start != string::npos && val_end != string::npos){
            value = value.substr(val_start, val_end - val_start + 1);
        }

        Condition cond;
        cond.column = column;
        cond.op = op;
        cond.value = value;
        conditions.push_back(cond);
    }
}

bool checkConditions(const json& entry, const vector<Condition>& conditions, const string& logical_op) {
    if (conditions.empty()) return true;

    if (logical_op == "AND") {
        for (const auto& cond : conditions) {
            if (!entry.contains(cond.column)) return false;

            string entry_value = entry[cond.column].get<string>();
            bool condition_met = false;
            if (cond.op == "=") {
                condition_met = (entry_value == cond.value);
            }
            else if (cond.op == "!=") {
                condition_met = (entry_value != cond.value);
            }
            else if (cond.op == "<") {
                condition_met = (entry_value < cond.value);
            }
            else if (cond.op == ">") {
                condition_met = (entry_value > cond.value);
            }
            else if (cond.op == "<=") {
                condition_met = (entry_value <= cond.value);
            }
            else if (cond.op == ">=") {
                condition_met = (entry_value >= cond.value);
            }

            if (!condition_met) return false;
        }
        return true;
    }
    else if (logical_op == "OR") {
        for (const auto& cond : conditions) {
            if (!entry.contains(cond.column)) continue;

            string entry_value = entry[cond.column].get<string>();
            bool condition_met = false;
            if (cond.op == "=") {
                condition_met = (entry_value == cond.value);
            }
            else if (cond.op == "!=") {
                condition_met = (entry_value != cond.value);
            }
            else if (cond.op == "<") {
                condition_met = (entry_value < cond.value);
            }
            else if (cond.op == ">") {
                condition_met = (entry_value > cond.value);
            }
            else if (cond.op == "<=") {
                condition_met = (entry_value <= cond.value);
            }
            else if (cond.op == ">=") {
                condition_met = (entry_value >= cond.value);
            }

            if (condition_met) return true;
        }
        return false;
    }
    else {
        // Если логический оператор не определен, предполагаем AND между условиями
        for (const auto& cond : conditions) {
            if (!entry.contains(cond.column)) return false;

            string entry_value = entry[cond.column].get<string>();
            bool condition_met = false;
            if (cond.op == "=") {
                condition_met = (entry_value == cond.value);
            }
            else if (cond.op == "!=") {
                condition_met = (entry_value != cond.value);
            }
            else if (cond.op == "<") {
                condition_met = (entry_value < cond.value);
            }
            else if (cond.op == ">") {
                condition_met = (entry_value > cond.value);
            }
            else if (cond.op == "<=") {
                condition_met = (entry_value <= cond.value);
            }
            else if (cond.op == ">=") {
                condition_met = (entry_value >= cond.value);
            }

            if (!condition_met) return false;
        }
        return true;
    }
}

// ---------------------------------------------------------------------------
// Функция для выборки из таблицы (без условий JOIN)
// ---------------------------------------------------------------------------
void selectFromTable(dbase& db, const string& table, const vector<string>& columns,
                     const vector<Condition>& conditions, const string& logical_op,
                     stringstream& result_stream)
{
    Node* table_node = db.findNode(table);
    
    if (!table_node) {
        result_stream << "Table not found: " << table << "\n";
        return;
    }

    bool data_found = false;

    // Вывод "заголовка"
    for (size_t i = 0; i < columns.size(); ++i) {
        result_stream << columns[i];
        if (i < columns.size() - 1) result_stream << ", ";
    }
    result_stream << "\n";

    // Перебираем все записи в данной таблице
    for (const auto& record_str : table_node->data) {
        json entry = json::parse(record_str);

        // Проверяем WHERE-условия
        if (checkConditions(entry, conditions, logical_op)) {
            data_found = true;
            // Вывод указанных столбцов
            for (size_t j = 0; j < columns.size(); ++j) {
                if (columns[j].find('.') != string::npos) {
                    // table1.name
                    size_t dot_pos = columns[j].find('.');
                    string col = columns[j].substr(dot_pos + 1);
                    if (entry.contains(col)) {
                        result_stream << entry[col].get<string>();
                    } else {
                        result_stream << "NULL";
                    }
                }
                else if (columns[j] == "*") {
                    // Вывод всех столбцов
                    bool first = true;
                    for (auto it = entry.begin(); it != entry.end(); ++it) {
                        if (!first) result_stream << ", ";
                        result_stream << it.key() << ": \"" << it.value().get<string>() << "\"";
                        first = false;
                    }
                    break; // уже всё вывели
                } else {
                    // Просто вывод значения
                    if (entry.contains(columns[j])) {
                        result_stream << entry[columns[j]].get<string>();
                    } else {
                        result_stream << "NULL";
                    }
                }
                if (j < columns.size() - 1) result_stream << ", ";
            }
            result_stream << ";\n";
        }
    }

    if (!data_found) {
        result_stream << "No data found in the table: " << table
                      << " with the specified conditions.\n";
    }
}

// ---------------------------------------------------------------------------
// Функция для выборки из нескольких таблиц (UNION)
// ---------------------------------------------------------------------------
void selectFromMultipleTables(dbase& db, const vector<string>& columns,
                              const vector<string>& tables,
                              const vector<Condition>& conditions,
                              const string& logical_op,
                              stringstream& result_stream)
{
    if (tables.empty()) {
        result_stream << "No tables specified for SELECT.\n";
        return;
    }

    // Вывод "заголовка"
    for (size_t i = 0; i < columns.size(); ++i) {
        result_stream << columns[i];
        if (i < columns.size() - 1) result_stream << ", ";
    }
    result_stream << "\n";

    bool data_found = false;

    // Перебираем все указанные таблицы
    for (const auto& tbl : tables) {
        Node* table_node = db.findNode(tbl);
        if (!table_node) {
            result_stream << "Table not found: " << tbl << "\n";
            continue;
        }

        for (const auto& record_str : table_node->data) {
            json entry = json::parse(record_str);

            // Проверка WHERE
            if (checkConditions(entry, conditions, logical_op)) {
                data_found = true;

                // Вывод указанных столбцов
                for (size_t j = 0; j < columns.size(); ++j) {
                    if (columns[j].find('.') != string::npos) {
                        // table1.name
                        size_t dot_pos = columns[j].find('.');
                        string col = columns[j].substr(dot_pos + 1);
                        if (entry.contains(col)) {
                            result_stream << entry[col].get<string>();
                        } else {
                            result_stream << "NULL";
                        }
                    }
                    else if (columns[j] == "*") {
                        // Все столбцы
                        bool first = true;
                        for (auto it = entry.begin(); it != entry.end(); ++it) {
                            if (!first) result_stream << ", ";
                            result_stream << it.key() << ": \"" << it.value().get<string>() << "\"";
                            first = false;
                        }
                        break; 
                    } else {
                        if (entry.contains(columns[j])) {
                            result_stream << entry[columns[j]].get<string>();
                        } else {
                            result_stream << "NULL";
                        }
                    }
                    if (j < columns.size() - 1) result_stream << ", ";
                }
                result_stream << ";\n";
            }
        }
    }

    if (!data_found) {
        result_stream << "No data found in the specified tables with the given conditions.\n";
    }
}

// ---------------------------------------------------------------------------
// Функция для выполнения CROSS JOIN между двумя таблицами (единая версия!)
// ---------------------------------------------------------------------------
void crossJoinTables(dbase& db,
                     const string& table1,
                     const string& table2,
                     const vector<string>& columns,
                     const vector<Condition>& conditions,
                     const string& logical_op,
                     stringstream& result_stream)
{
    Node* tbl1 = db.findNode(table1);
    Node* tbl2 = db.findNode(table2);

    if (!tbl1) {
        result_stream << "Table not found: " << table1 << "\n";
        return;
    }
    if (!tbl2) {
        result_stream << "Table not found: " << table2 << "\n";
        return;
    }

    // Специальная обработка для случая, когда запрашивается только "name"
    bool select_only_name = false;
    if (columns.size() == 1 && columns[0] == "name") {
        select_only_name = true;
        result_stream << "name1 name2\n"; // Шапка
    } else {
        // Вывод "заголовка"
        for (size_t i = 0; i < columns.size(); ++i) {
            result_stream << columns[i];
            if (i < columns.size() - 1) result_stream << ", ";
        }
        result_stream << "\n";
    }

    bool data_found = false;

    // Декартово произведение
    for (const auto& record1_str : tbl1->data) {
        json entry1 = json::parse(record1_str);
        for (const auto& record2_str : tbl2->data) {
            json entry2 = json::parse(record2_str);

            // Объединяем JSON
            json combined_entry = entry1;
            for (auto it = entry2.begin(); it != entry2.end(); ++it) {
                if (combined_entry.contains(it.key())) {
                    combined_entry["." + it.key()] = it.value();
                } else {
                    combined_entry[it.key()] = it.value();
                }
            }

            // WHERE
            if (checkConditions(combined_entry, conditions, logical_op)) {
                data_found = true;

                if (select_only_name) {
                    // Вывод: misha nikita
                    string name1 = entry1.contains("name") ? entry1["name"].get<string>() : "NULL";
                    string name2 = entry2.contains("name") ? entry2["name"].get<string>() : "NULL";
                    result_stream << name1 << " " << name2 << "\n";
                }
                else {
                    // Вывод столбцов
                    for (size_t j = 0; j < columns.size(); ++j) {
                        if (columns[j].find('.') != string::npos) {
                            size_t dot_pos = columns[j].find('.');
                            string col = columns[j].substr(dot_pos + 1);
                            if (combined_entry.contains(col)) {
                                result_stream << combined_entry[col].get<string>();
                            } else {
                                result_stream << "NULL";
                            }
                        }
                        else if (columns[j] == "*") {
                            // все
                            bool first = true;
                            for (auto it = combined_entry.begin(); it != combined_entry.end(); ++it) {
                                if (!first) result_stream << ", ";
                                result_stream << it.key() << ": \"" << it.value().get<string>() << "\"";
                                first = false;
                            }
                            break; 
                        } else {
                            if (combined_entry.contains(columns[j])) {
                                result_stream << combined_entry[columns[j]].get<string>();
                            } else {
                                result_stream << "NULL";
                            }
                        }
                        if (j < columns.size() - 1) result_stream << ", ";
                    }
                    result_stream << ";\n";
                }
            }
        }
    }

    if (!data_found) {
        result_stream << "No data found after CROSS JOIN with the specified conditions.\n";
    }
}

// ---------------------------------------------------------------------------
// Функция handleClient (обработка команд INSERT, SELECT, DELETE и т.п.)
// ---------------------------------------------------------------------------
void handleClient(int client_socket, dbase& db) {
    char buffer[4096];
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        int bytes_read = read(client_socket, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) {
            cout << "Client disconnected or read error.\n";
            break; 
        }
        string query(buffer);

        // Удаляем перевод строки
        size_t newline_pos = query.find_first_of("\r\n");
        if (newline_pos != string::npos) {
            query = query.substr(0, newline_pos);
        }

        istringstream iss(query);
        string action;
        iss >> action;

        // Приведение команды к верхнему регистру
        transform(action.begin(), action.end(), action.begin(), ::toupper);

        try {
            if (action == "INSERT") {
                // ... ваш код INSERT ...
                string table;
                iss >> table;

                vector<string> args;
                string arg;
                while (iss >> ws && getline(iss, arg, ' ')) {
                    if (!arg.empty() && arg.front() == '"' && arg.back() == '"') {
                        arg = arg.substr(1, arg.size() - 2);
                    }
                    args.push_back(arg);
                }

                size_t expected_arg_count = db.getColumnCount(table);
                if (args.size() > expected_arg_count) {
                    string error_message = "Error: Too many arguments for INSERT command.\n";
                    send(client_socket, error_message.c_str(), error_message.size(), 0);
                    continue;
                } else if (args.size() < 2) {
                    string error_message = "Error: Not enough arguments for INSERT command.\n";
                    send(client_socket, error_message.c_str(), error_message.size(), 0);
                    continue;
                }

                json entry;
                entry["name"] = args[0];
                entry["age"]  = args[1];

                if (args.size() > 2) {
                    entry["adress"] = args[2];
                } else {
                    entry["adress"] = "";
                }

                if (args.size() > 3) {
                    entry["number"] = args[3];
                } else {
                    entry["number"] = "";
                }

                insertRecord(db, table, entry);
                string success_message = "Data successfully inserted.\n";
                send(client_socket, success_message.c_str(), success_message.size(), 0);
            }
            else if (action == "SELECT") {
                // Логика SELECT ...
                size_t from_pos = query.find("FROM");
                if (from_pos == string::npos) {
                    string error_message = "Error: SELECT command must contain FROM.\n";
                    send(client_socket, error_message.c_str(), error_message.size(), 0);
                    continue;
                }

                // Часть SELECT ... FROM ...
                string select_part = query.substr(0, from_pos);
                string from_part   = query.substr(from_pos);

                // Парсим столбцы
                vector<string> columns;
                {
                    size_t select_start = select_part.find("SELECT");
                    if (select_start != string::npos) {
                        string cols_str = select_part.substr(select_start + 6);
                        size_t start_pos = 0;
                        while (start_pos < cols_str.length()) {
                            size_t comma = cols_str.find(',', start_pos);
                            if (comma != string::npos) {
                                string col = cols_str.substr(start_pos, comma - start_pos);
                                // trim
                                size_t col_start = col.find_first_not_of(" \t");
                                size_t col_end   = col.find_last_not_of(" \t");
                                if(col_start != string::npos && col_end != string::npos){
                                    col = col.substr(col_start, col_end - col_start + 1);
                                }
                                columns.push_back(col);
                                start_pos = comma + 1;
                            }
                            else {
                                string col = cols_str.substr(start_pos);
                                size_t col_start = col.find_first_not_of(" \t");
                                size_t col_end   = col.find_last_not_of(" \t");
                                if(col_start != string::npos && col_end != string::npos){
                                    col = col.substr(col_start, col_end - col_start + 1);
                                }
                                if (!col.empty()) {
                                    columns.push_back(col);
                                }
                                break;
                            }
                        }
                    }
                }

                // Парсинг части FROM ... (включая CROSS JOIN)
                istringstream from_stream(from_part);
                string from_keyword; // должно быть 'FROM'
                from_stream >> from_keyword;

                // table1 ...
                string table1, join_type, table2;
                from_stream >> table1;

                bool is_cross_join = false;
                size_t cross_pos = table1.find("CROSS");
                if (cross_pos != string::npos) {
                    // пример: table1CROSS JOIN table2
                    string actual_table1 = table1.substr(0, cross_pos);
                    // trim
                    size_t t_start = actual_table1.find_first_not_of(" \t");
                    size_t t_end   = actual_table1.find_last_not_of(" \t");
                    if(t_start != string::npos && t_end != string::npos){
                        actual_table1 = actual_table1.substr(t_start, t_end - t_start + 1);
                    }
                    table1 = actual_table1;
                    is_cross_join = true;

                    from_stream >> join_type; // JOIN
                    if (join_type != "JOIN") {
                        string error_message = "Error: Invalid CROSS JOIN syntax.\n";
                        send(client_socket, error_message.c_str(), error_message.size(), 0);
                        continue;
                    }
                    from_stream >> table2;
                } else {
                    // Проверка, есть ли отдельно "CROSS JOIN"
                    from_stream >> join_type;
                    if (join_type == "CROSS") {
                        // ожидаем JOIN
                        string join_keyword;
                        from_stream >> join_keyword; 
                        if (join_keyword != "JOIN") {
                            string error_message = "Error: Invalid CROSS JOIN syntax.\n";
                            send(client_socket, error_message.c_str(), error_message.size(), 0);
                            continue;
                        }
                        from_stream >> table2;
                        is_cross_join = true;
                    } else {
                        // нет JOIN
                        // возвращаемся
                        from_stream.seekg(-static_cast<int>(join_type.length()), ios_base::cur);
                        join_type = "";
                    }
                }

                // Остаток после JOIN (WHERE и т.д.)
                string remaining;
                getline(from_stream, remaining);

                // Проверяем WHERE
                string where_clause;
                bool has_where = false;
                size_t where_pos = remaining.find("WHERE");
                if (where_pos != string::npos) {
                    has_where = true;
                    where_clause = remaining.substr(where_pos + 5);
                }

                vector<Condition> conditions;
                string logical_op;
                if (has_where) {
                    parseWhereClause(where_clause, conditions, logical_op);
                }

                // Формируем результат
                stringstream result_stream;

                if (is_cross_join) {
                    // CROSS JOIN
                    crossJoinTables(db, table1, table2, columns, conditions, logical_op, result_stream);
                } else {
                    // Нет CROSS JOIN. Может быть UNION
                    vector<string> temp_tables;
                    temp_tables.push_back(table1);
                    if (!join_type.empty() && !table2.empty()) {
                        // Возможно, кто-то написал что-то вроде: table1 JOIN table2
                        // Но это не CROSS, игнорируем/обрабатываем как UNION
                        temp_tables.push_back(join_type);
                        temp_tables.push_back(table2);
                    }

                    if (temp_tables.size() == 1) {
                        // Обычный SELECT из одной таблицы
                        selectFromTable(db, temp_tables[0], columns, conditions, logical_op, result_stream);
                    }
                    else if (temp_tables.size() > 1) {
                        // Выборка из нескольких таблиц (UNION)
                        selectFromMultipleTables(db, columns, temp_tables, conditions, logical_op, result_stream);
                    }
                }

                // Отправка результата
                string result = result_stream.str();
                send(client_socket, result.c_str(), result.size(), 0);
            }
            else if (action == "DELETE") {
                // DELETE FROM table column value
                string from_word, table, column, value;
                iss >> from_word >> table >> column >> value;
                transform(from_word.begin(), from_word.end(), from_word.begin(), ::toupper);
                if (from_word != "FROM") {
                    string error_message = "Error: Invalid DELETE syntax.\n";
                    send(client_socket, error_message.c_str(), error_message.size(), 0);
                    continue;
                }
                deleteRow(db, column, value, table);
                string success_message = "Row deleted successfully.\n";
                send(client_socket, success_message.c_str(), success_message.size(), 0);
            }
            else if (action == "EXIT") {
                cout << "Client requested to close the connection.\n";
                break;
            }
            else {
                string error_message = "Unknown command: " + query + "\n";
                send(client_socket, error_message.c_str(), error_message.size(), 0);
            }
        }
        catch (const exception& e) {
            string error_message = "Error: " + string(e.what()) + "\n";
            send(client_socket, error_message.c_str(), error_message.size(), 0);
        }
    }

    // Закрываем соединение, завершаем handleClient
    close(client_socket);
    cout << "Connection with client closed.\n";
}

// ---------------------------------------------------------------------------
// Функция main()
// ---------------------------------------------------------------------------
int main() {
    dbase db;
    loadSchema(db, "schema.json");
    db.load();

    int server_socket;
    struct sockaddr_in server_addr;

    // Создание сокета
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        cerr << "Error creating socket." << endl;
        return 1;
    }

    // Настройка адреса сервера
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    // Измените порт на свободный, если 7432 занят
    server_addr.sin_port = htons(7432);

    // Привязка сокета к адресу
    if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        cerr << "Error binding socket. Port might be in use." << endl;
        close(server_socket);
        return 1;
    }

    // Ожидание подключения клиентов
    listen(server_socket, 5);
    cout << "Server listening on port 7432..." << endl;

    while (true) {
        int client_socket = accept(server_socket, nullptr, nullptr);
        if (client_socket < 0) {
            cerr << "Error accepting connection." << endl;
            continue;
        }
        cout << "Client connected." << endl;

        // Создаем новый поток для обработки клиента
        thread client_thread(handleClient, client_socket, ref(db));
        client_thread.detach(); // Поток отделяется
    }

    close(server_socket);
    return 0;
}
