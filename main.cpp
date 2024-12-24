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
#include "json.hpp" // Библиотека для работы с JSON
#include <sstream> // Для istringstream
#include <vector>

using namespace std;
using json = nlohmann::json;

// Структура для представления таблицы
struct Node {
    string name;
    vector<string> data; // Хранение данных в формате JSON-строк
    Node* next;

    Node(const string& name) : name(name), next(nullptr) {}
};

// Структура для базы данных
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
                            // Удаление начальных и конечных пробелов
                            size_t f_start = field.find_first_not_of(" \t");
                            size_t f_end = field.find_last_not_of(" \t");
                            if(f_start != string::npos && f_end != string::npos){
                                field = field.substr(f_start, f_end - f_start + 1);
                            }
                            if (!field.empty()) {
                                fields.push_back(field);
                            }
                        }

                        if (fields.size() == 4) {
                            json entry;
                            entry["name"] = fields[0];
                            entry["age"] = fields[1];
                            entry["adress"] = fields[2];
                            entry["number"] = fields[3];

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

// Структура для условий WHERE
struct Condition {
    string column;
    string op; // Например, =, <, >, != и т.д.
    string value;
};

// Функция для создания директорий согласно схеме
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

// Функция загрузки схемы из файла
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

// Функция для сохранения одной записи в CSV
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

// Функция для вставки записи
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

// Функция для удаления записи
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

// Функция для парсинга условий WHERE
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

        // Добавление условия в вектор
        Condition cond;
        cond.column = column;
        cond.op = op;
        cond.value = value;
        conditions.push_back(cond);
    }
}

// Функция для проверки соответствия записи условиям
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

// Функция для выборки из таблицы без условий
void selectFromTable(dbase& db, const string& table, const vector<string>& columns, stringstream& result_stream) {
    Node* table_node = db.findNode(table);
    
    if (!table_node) {
        result_stream << "Table not found: " << table << "\n";
        return;
    }

    bool data_found = false;

    // Вывод заголовка
    for (size_t i = 0; i < columns.size(); ++i) {
        result_stream << columns[i];
        if (i < columns.size() - 1) result_stream << ", ";
    }
    result_stream << "\n";

    for (const auto& record_str : table_node->data) {
        json entry = json::parse(record_str);

        bool row_matches = true; // Без условий

        if (row_matches) {
            data_found = true; // Мы нашли хотя бы одну запись

            // Вывод указанных столбцов
            for (size_t j = 0; j < columns.size(); ++j) {
                if (columns[j] == "*") {
                    // Вывод всех столбцов
                    bool first = true;
                    for (auto it = entry.begin(); it != entry.end(); ++it) {
                        if (!first) result_stream << ", ";
                        result_stream << it.key() << ": \"" << it.value().get<string>() << "\"";
                        first = false;
                    }
                    break; // Завершаем, так как уже вывели все столбцы
                } else {
                    if (entry.contains(columns[j])) {
                        result_stream << entry[columns[j]].get<string>();
                    } else {
                        result_stream << "NULL";
                    }
                    if (j < columns.size() - 1) result_stream << ", ";
                }
            }
            result_stream << ";\n";
        }
    }

    if (!data_found) {
        result_stream << "No data found in the table: " << table << "\n";
    }
}

// Функция для выборки из таблицы с условиями
void selectFromTableWithConditions(dbase& db, const string& table, const vector<string>& columns, const vector<Condition>& conditions, const string& logical_op, stringstream& result_stream) {
    Node* table_node = db.findNode(table);
    
    if (!table_node) {
        result_stream << "Table not found: " << table << "\n";
        return;
    }

    bool data_found = false;

    // Вывод заголовка
    for (size_t i = 0; i < columns.size(); ++i) {
        result_stream << columns[i];
        if (i < columns.size() - 1) result_stream << ", ";
    }
    result_stream << "\n";

    for (const auto& record_str : table_node->data) {
        json entry = json::parse(record_str);

        // Проверка условий
        if (checkConditions(entry, conditions, logical_op)) {
            data_found = true;

            // Вывод указанных столбцов
            for (size_t j = 0; j < columns.size(); ++j) {
                if (columns[j] == "*") {
                    // Вывод всех столбцов
                    bool first = true;
                    for (auto it = entry.begin(); it != entry.end(); ++it) {
                        if (!first) result_stream << ", ";
                        result_stream << it.key() << ": \"" << it.value().get<string>() << "\"";
                        first = false;
                    }
                    break; // Завершаем, так как уже вывели все столбцы
                } else {
                    if (entry.contains(columns[j])) {
                        result_stream << entry[columns[j]].get<string>();
                    } else {
                        result_stream << "NULL";
                    }
                    if (j < columns.size() - 1) result_stream << ", ";
                }
            }
            result_stream << ";\n";
        }
    }

    if (!data_found) {
        result_stream << "No data found in the table: " << table << " with the specified conditions.\n";
    }
}

// Функция для выборки из нескольких таблиц (CROSS JOIN)
void selectFromMultipleTables(dbase& db, const vector<string>& columns, const vector<string>& tables, const vector<Condition>& conditions, const string& logical_op, stringstream& result_stream) {
    if (tables.size() < 2) {
        result_stream << "Need at least two tables for multiple table SELECT.\n";
        return;
    }

    // Реализация CROSS JOIN между двумя таблицами
    Node* table_node1 = db.findNode(tables[0]);
    Node* table_node2 = db.findNode(tables[1]);

    if (!table_node1 || !table_node2) {
        if (!table_node1) result_stream << "Table not found: " << tables[0] << "\n";
        if (!table_node2) result_stream << "Table not found: " << tables[1] << "\n";
        return;
    }

    bool data_found = false;

    // Вывод заголовка
    for (size_t i = 0; i < columns.size(); ++i) {
        result_stream << columns[i];
        if (i < columns.size() - 1) result_stream << ", ";
    }
    result_stream << "\n";

    for (const auto& record1_str : table_node1->data) {
        json entry1 = json::parse(record1_str);
        cout << "Processing entry from table " << tables[0] << ": " << entry1.dump() << endl; // Отладка

        for (const auto& record2_str : table_node2->data) {
            json entry2 = json::parse(record2_str);
            cout << "Processing entry from table " << tables[1] << ": " << entry2.dump() << endl; // Отладка

            // Объединение двух записей
            json combined = entry1;
            for (auto it = entry2.begin(); it != entry2.end(); ++it) {
                // Предотвращаем конфликты ключей
                string key = it.key();
                if (combined.contains(key)) {
                    key = tables[1] + "." + key;
                }
                combined[key] = it.value();
            }

            // Проверка условий
            bool match = false;
            if (!conditions.empty()) {
                match = checkConditions(combined, conditions, logical_op);
            }
            else {
                match = true; // Без условий
            }

            if (match) {
                data_found = true;
                // Вывод указанных столбцов
                for (size_t k = 0; k < columns.size(); ++k) {
                    if (columns[k] == "*") {
                        // Вывод всех столбцов
                        bool first = true;
                        for (auto it = combined.begin(); it != combined.end(); ++it) {
                            if (!first) result_stream << ", ";
                            result_stream << it.key() << ": \"" << it.value().get<string>() << "\"";
                            first = false;
                        }
                        break;
                    }
                    else {
                        if (combined.contains(columns[k])) {
                            result_stream << combined[columns[k]].get<string>();
                        }
                        else {
                            result_stream << "NULL";
                        }
                        if (k < columns.size() - 1) result_stream << ", ";
                    }
                }
                result_stream << ";\n";
                cout << "Matched combination: " << combined.dump() << endl; // Отладка
            }
        }
    }

    if (!data_found) {
        result_stream << "No data found in the cross join of the specified tables with the given conditions.\n";
    }
}

// Функция для обработки клиента
void handleClient(int client_socket, dbase& db) {
    char buffer[4096]; // Увеличен размер буфера для больших запросов
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        int bytes_read = read(client_socket, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) {
            cout << "Client disconnected or read error.\n";
            break; // Выход из цикла при ошибке или закрытии соединения
        }
        string query(buffer);
        istringstream iss(query);
        string action;
        iss >> action;

        // Приведение команды к верхнему регистру для устойчивости
        for (auto& c : action) c = toupper(c);

        try {
            if (action == "INSERT") {
                string table;
                iss >> table;

                vector<string> args;
                string arg;

                // Чтение всех оставшихся аргументов
                while (iss >> arg) {
                    args.push_back(arg);
                }

                size_t expected_arg_count = db.getColumnCount(table) - 1; // Учитываем 'id' как автоинкремент
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
                entry["age"] = args[1];

                if (args.size() > 2) {
                    entry["adress"] = args[2];
                } else {
                    entry["adress"] = ""; // Значение по умолчанию
                }

                if (args.size() > 3) {
                    entry["number"] = args[3];
                } else {
                    entry["number"] = ""; // Значение по умолчанию
                }

                insertRecord(db, table, entry);
                string success_message = "Data successfully inserted.\n";
                send(client_socket, success_message.c_str(), success_message.size(), 0);

            }
            else if (action == "SELECT") {
                string select_part;
                // Чтение до 'FROM'
                getline(iss, select_part, 'F'); // 'F' из 'FROM'
                string from_str;
                iss >> from_str; // Остальные символы из 'FROM'

                if (from_str.substr(0, 3) != "ROM") { // Проверка на корректность 'FROM'
                    string error_message = "Error: Invalid SELECT syntax.\n";
                    send(client_socket, error_message.c_str(), error_message.size(), 0);
                    continue;
                }

                // Чтение списка таблиц, разделённых запятыми, до ключевого слова 'WHERE'
                vector<string> tables;
                string table;
                while (iss >> table) {
                    // Проверка на наличие 'WHERE'
                    if (table == "WHERE") {
                        break; // Прекращаем чтение таблиц
                    }

                    // Удаление запятых
                    size_t comma_pos = table.find(',');
                    if (comma_pos != string::npos) {
                        string tbl = table.substr(0, comma_pos);
                        // Удаление пробелов
                        size_t tbl_start = tbl.find_first_not_of(" \t");
                        size_t tbl_end = tbl.find_last_not_of(" \t");
                        if(tbl_start != string::npos && tbl_end != string::npos){
                            tbl = tbl.substr(tbl_start, tbl_end - tbl_start + 1);
                        }
                        if(!tbl.empty()){
                            tables.push_back(tbl);
                        }
                        string remaining = table.substr(comma_pos + 1);
                        // Удаление пробелов
                        size_t rem_start = remaining.find_first_not_of(" \t");
                        size_t rem_end = remaining.find_last_not_of(" \t");
                        if(rem_start != string::npos && rem_end != string::npos){
                            remaining = remaining.substr(rem_start, rem_end - rem_start + 1);
                        }
                        if(!remaining.empty()){
                            tables.push_back(remaining);
                        }
                    }
                    else {
                        // Удаление пробелов
                        size_t tbl_start = table.find_first_not_of(" \t");
                        size_t tbl_end = table.find_last_not_of(" \t");
                        if(tbl_start != string::npos && tbl_end != string::npos){
                            table = table.substr(tbl_start, tbl_end - tbl_start + 1);
                        }
                        if(!table.empty()){
                            tables.push_back(table);
                        }
                    }
                }

                // Проверка наличия 'WHERE'
                string token;
                string where_clause;
                bool has_where = false;
                if (table == "WHERE") { // Если 'WHERE' уже был прочитан
                    has_where = true;
                    getline(iss, where_clause);
                }
                else {
                    // Возврат позиции, если не 'WHERE'
                    iss.clear();
                    iss.seekg(-static_cast<int>(table.size()), ios_base::cur);
                }

                // Парсинг выбранных столбцов
                vector<string> columns;
                // Удаление возможных пробелов и запятых
                size_t pos = select_part.find("SELECT");
                if (pos != string::npos) {
                    select_part = select_part.substr(pos + 6);
                }
                // Разделение столбцов по запятым
                size_t start_pos = 0;
                while (start_pos < select_part.length()) {
                    size_t comma = select_part.find(',', start_pos);
                    if (comma != string::npos) {
                        string col = select_part.substr(start_pos, comma - start_pos);
                        // Удаление пробелов
                        size_t col_start = col.find_first_not_of(" \t");
                        size_t col_end = col.find_last_not_of(" \t");
                        if(col_start != string::npos && col_end != string::npos){
                            col = col.substr(col_start, col_end - col_start + 1);
                        }
                        columns.push_back(col);
                        start_pos = comma + 1;
                    }
                    else {
                        string col = select_part.substr(start_pos);
                        // Удаление пробелов
                        size_t col_start = col.find_first_not_of(" \t");
                        size_t col_end = col.find_last_not_of(" \t");
                        if(col_start != string::npos && col_end != string::npos){
                            col = col.substr(col_start, col_end - col_start + 1);
                        }
                        if (!col.empty()) {
                            columns.push_back(col);
                        }
                        break;
                    }
                }

                // Парсинг условий WHERE, если есть
                vector<Condition> conditions;
                string logical_op;
                if (has_where) {
                    parseWhereClause(where_clause, conditions, logical_op);
                }

                // Выполнение выборки
                stringstream result_stream;
                if (tables.size() == 1) {
                    if (conditions.empty()) {
                        // Выборка без условий
                        selectFromTable(db, tables[0], columns, result_stream);
                    }
                    else {
                        // Выборка с условиями
                        selectFromTableWithConditions(db, tables[0], columns, conditions, logical_op, result_stream);
                    }
                }
                else if (tables.size() == 2) {
                    // Выборка с CROSS JOIN между двумя таблицами
                    selectFromMultipleTables(db, columns, tables, conditions, logical_op, result_stream);
                }
                else {
                    // Для простоты, поддержка только двух таблиц в CROSS JOIN
                    string error_message = "Error: Only two tables supported for CROSS JOIN.\n";
                    send(client_socket, error_message.c_str(), error_message.size(), 0);
                    continue;
                }

                string result = result_stream.str();
                send(client_socket, result.c_str(), result.size(), 0);

            }
            else if (action == "DELETE") {
                string from_word, table, column, value;
                iss >> from_word >> table >> column >> value;
                // Приведение к верхнему регистру для устойчивости
                for (auto& c : from_word) c = toupper(c);
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
                break; // Выход из цикла по запросу клиента
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
    } // Завершение цикла while

    // Закрыть соединение с клиентом после выхода из цикла
    close(client_socket);
    cout << "Connection with client closed.\n";
}

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
    server_addr.sin_port = htons(7432);

    // Привязка сокета к адресу
    if (bind(server_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        cerr << "Error binding socket." << endl;
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
        client_thread.detach(); // Отделяем поток, чтобы он мог работать независимо
    }

    close(server_socket);
    return 0;
}
