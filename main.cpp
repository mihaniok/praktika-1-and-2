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

using namespace std;
using json = nlohmann::json;

// Шаблон структуры для парсинга пар ключ-значение
template<typename T1, typename T2>
struct Pars {
    T1 first;
    T2 second;

    Pars() : first(T1()), second(T2()) {} // Конструктор по умолчанию
    Pars(const T1& f, const T2& s) : first(f), second(s) {}
};

// Структура динамического массива строк
struct Array {
    string* arr;
    size_t capacity;
    size_t size;

    Array() : capacity(10), size(0) {
        arr = new string[capacity];
    }

    ~Array() {
        delete[] arr;
    }

    void addEnd(const string& value) {
        if (size >= capacity) {
            capacity *= 2;
            string* new_arr = new string[capacity];
            for (size_t i = 0; i < size; ++i) {
                new_arr[i] = arr[i];
            }
            delete[] arr;
            arr = new_arr;
        }
        arr[size++] = value;
    }

    string get(size_t index) const {
        if (index >= size) throw out_of_range("Index out of range");
        return arr[index];
    }

    size_t getSize() const {
        return size;
    }
};

// Структура для представления таблицы
struct Node {
    string name;
    Array data;
    Node* next;

    Node(const string& name) : name(name), next(nullptr) {}
};

// Структура для базы данных
struct dbase {
    string filename; 
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
                filename = schema_name + "/" + current->name + "/1.csv"; 
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
                        Array fields;
                        string field;

                        while (getline(iss, field, ',')) {
                            // Удаление начальных и конечных пробелов
                            size_t f_start = field.find_first_not_of(" \t");
                            size_t f_end = field.find_last_not_of(" \t");
                            if(f_start != string::npos && f_end != string::npos){
                                field = field.substr(f_start, f_end - f_start + 1);
                            }
                            if (!field.empty()) {
                                fields.addEnd(field);
                            }
                        }

                        if (fields.getSize() == 4) {
                            json entry;
                            entry["name"] = fields.get(0);
                            entry["age"] = fields.get(1);
                            entry["adress"] = fields.get(2);
                            entry["number"] = fields.get(3);

                            current->data.addEnd(entry.dump());
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
            db.filename = table_path + "/1.csv";

            ifstream check_file(db.filename);
            if (!check_file) {
                ofstream file(db.filename);
                if (file.is_open()) {
                    auto& columns = it.value();
                    string header = "";
                    for (size_t i = 0; i < columns.size(); ++i) {
                        header += columns[i].get<string>();
                        if (i < columns.size() - 1) header += ", ";
                    }
                    file << header << "\n";
                    file.close();
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
                cout << "Data successfully saved for: " << entry.dump() << endl;
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
void insert(dbase& db, const string& table, json entry) {
    Node* table_node = db.findNode(table);
    if (table_node) {
        entry["id"] = to_string(db.current_pk); 
        db.current_pk++; // Увеличение первичного ключа

        table_node->data.addEnd(entry.dump());

        saveSingleEntryToCSV(db, table, entry);
    } else {
        cout << "Table not found: " << table << endl;
    }
}

// Функция для удаления записи
void deleteRow(dbase& db, const string& column, const string& value, const string& table) {
    Node* table_node = db.findNode(table);
    if (table_node) {
        Array new_data;
        bool found = false;

        for (size_t i = 0; i < table_node->data.getSize(); ++i) {
            json entry = json::parse(table_node->data.get(i));
            if (entry.contains(column) && entry[column].get<string>() == value) {
                found = true;
                cout << "Deleted row: " << entry.dump() << endl;
            } else {
                new_data.addEnd(table_node->data.get(i));
            }
        }

        if (found) {
            table_node->data = new_data;
            // Перезапись CSV файла
            try {
                db.filename = db.schema_name + "/" + table + "/1.csv"; 
                ofstream file(db.filename); 

                if (file) {
                    // Запись заголовка
                    file << "name, age, adress, number\n";

                    for (size_t i = 0; i < table_node->data.getSize(); ++i) {
                        json entry = json::parse(table_node->data.get(i));
                        file << entry["name"].get<string>() << ", "
                             << entry["age"].get<string>() << ", "
                             << entry["adress"].get<string>() << ", "
                             << entry["number"].get<string>() << "\n";
                    }
                    file.close();
                } else {
                    throw runtime_error("Failed to open data file for rewriting: " + db.filename);
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

// Функция для применения фильтра
bool applyFilter(const json& entry, const Pars<string, string>& filter) {
    const string& filter_column = filter.first; // Получаем ключ фильтра
    const string& filter_value = filter.second; // Получаем значение фильтра

    // Проверяем, есть ли колонка в записи и совпадает ли значение
    return entry.contains(filter_column) && entry[filter_column].get<string>() == filter_value;
}

// Функция для парсинга условий WHERE
void parseWhereClause(const string& where_clause, Array& conditions, string& logical_op) {
    // Разделяем условия по AND или OR
    size_t pos = where_clause.find(" AND ");
    if (pos != string::npos) {
        logical_op = "AND";
    } else {
        pos = where_clause.find(" OR ");
        if (pos != string::npos) {
            logical_op = "OR";
        } else {
            logical_op = "";
        }
    }

    size_t start = 0;
    while (start < where_clause.length()) {
        size_t end = where_clause.find(" AND ", start);
        if (end == string::npos) {
            end = where_clause.find(" OR ", start);
        }
        string condition_str;
        if (end != string::npos) {
            condition_str = where_clause.substr(start, end - start);
            start = end + 5; // Пропускаем " AND " или " OR "
        } else {
            condition_str = where_clause.substr(start);
            start = where_clause.length();
        }

        // Парсим отдельное условие
        size_t eq_pos = condition_str.find('=');
        size_t neq_pos = condition_str.find("!=");
        size_t lt_pos = condition_str.find('<');
        size_t gt_pos = condition_str.find('>');
        size_t le_pos = condition_str.find("<=");
        size_t ge_pos = condition_str.find(">=");

        Condition cond;
        if (le_pos != string::npos) {
            cond.op = "<=";
            cond.column = condition_str.substr(0, le_pos);
            cond.value = condition_str.substr(le_pos + 2);
        }
        else if (ge_pos != string::npos) {
            cond.op = ">=";
            cond.column = condition_str.substr(0, ge_pos);
            cond.value = condition_str.substr(ge_pos + 2);
        }
        else if (neq_pos != string::npos) {
            cond.op = "!=";
            cond.column = condition_str.substr(0, neq_pos);
            cond.value = condition_str.substr(neq_pos + 2);
        }
        else if (eq_pos != string::npos) {
            cond.op = "=";
            cond.column = condition_str.substr(0, eq_pos);
            cond.value = condition_str.substr(eq_pos + 1);
        }
        else if (lt_pos != string::npos) {
            cond.op = "<";
            cond.column = condition_str.substr(0, lt_pos);
            cond.value = condition_str.substr(lt_pos + 1);
        }
        else if (gt_pos != string::npos) {
            cond.op = ">";
            cond.column = condition_str.substr(0, gt_pos);
            cond.value = condition_str.substr(gt_pos + 1);
        }

        // Удаление пробелов и кавычек
        size_t c_start = cond.column.find_first_not_of(" \t");
        size_t c_end = cond.column.find_last_not_of(" \t");
        if(c_start != string::npos && c_end != string::npos){
            cond.column = cond.column.substr(c_start, c_end - c_start + 1);
        }

        size_t v_start = cond.value.find_first_not_of(" \t'");
        size_t v_end = cond.value.find_last_not_of(" \t'");
        if(v_start != string::npos && v_end != string::npos){
            cond.value = cond.value.substr(v_start, v_end - v_start + 1);
        }

        // Форматирование условия в JSON
        json cond_json;
        cond_json["column"] = cond.column;
        cond_json["op"] = cond.op;
        cond_json["value"] = cond.value;
        conditions.addEnd(cond_json.dump());
    }
}

// Функция для проверки соответствия записи условиям
bool checkConditions(const json& entry, const Array& conditions, const string& logical_op) {
    if (conditions.getSize() == 0) return true;

    bool result = (logical_op == "OR") ? false : true;

    for (size_t i = 0; i < conditions.getSize(); ++i) {
        json cond = json::parse(conditions.get(i));
        string column = cond["column"].get<string>();
        string op = cond["op"].get<string>();
        string value = cond["value"].get<string>();

        bool condition_met = false;
        if (entry.contains(column)) {
            string entry_value = entry[column].get<string>();
            if (op == "=") {
                condition_met = (entry_value == value);
            }
            else if (op == "!=") {
                condition_met = (entry_value != value);
            }
            else if (op == "<") {
                condition_met = (entry_value < value);
            }
            else if (op == ">") {
                condition_met = (entry_value > value);
            }
            else if (op == "<=") {
                condition_met = (entry_value <= value);
            }
            else if (op == ">=") {
                condition_met = (entry_value >= value);
            }
        }

        if (logical_op == "AND") {
            result = result && condition_met;
            if (!result) break; // Короткое замыкание
        }
        else if (logical_op == "OR") {
            result = result || condition_met;
            if (result) break; // Короткое замыкание
        }
        else {
            // Если логический оператор не определен, предполагаем AND
            result = result && condition_met;
            if (!result) break;
        }
    }

    return result;
}

// Функция для выборки из таблицы без условий
void selectFromTable(dbase& db, const string& table, const Array& columns, stringstream& result_stream) {
    Node* table_node = db.findNode(table);
    
    if (!table_node) {
        result_stream << "Table not found: " << table << "\n";
        return;
    }

    bool data_found = false;

    // Вывод заголовка
    for (size_t i = 0; i < columns.getSize(); ++i) {
        result_stream << columns.get(i);
        if (i < columns.getSize() - 1) result_stream << ", ";
    }
    result_stream << "\n";

    for (size_t i = 0; i < table_node->data.getSize(); ++i) {
        json entry = json::parse(table_node->data.get(i));

        bool row_matches = true; // Без условий

        if (row_matches) {
            data_found = true; // Мы нашли хотя бы одну запись

            // Вывод указанных столбцов
            for (size_t j = 0; j < columns.getSize(); ++j) {
                if (columns.get(j) == "*") {
                    // Вывод всех столбцов
                    bool first = true;
                    for (auto it = entry.begin(); it != entry.end(); ++it) {
                        if (!first) result_stream << ", ";
                        result_stream << it.key() << ": \"" << it.value().get<string>() << "\"";
                        first = false;
                    }
                    break; // Завершаем, так как уже вывели все столбцы
                } else {
                    if (entry.contains(columns.get(j))) {
                        result_stream << columns.get(j) << ": \"" << entry[columns.get(j)].get<string>() << "\"";
                    } else {
                        result_stream << columns.get(j) << ": NULL";
                    }
                    if (j < columns.getSize() - 1) result_stream << ", ";
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
void selectFromTableWithConditions(dbase& db, const string& table, const Array& columns, const Array& conditions, const string& logical_op, stringstream& result_stream) {
    Node* table_node = db.findNode(table);
    
    if (!table_node) {
        result_stream << "Table not found: " << table << "\n";
        return;
    }

    bool data_found = false;

    // Вывод заголовка
    for (size_t i = 0; i < columns.getSize(); ++i) {
        result_stream << columns.get(i);
        if (i < columns.getSize() - 1) result_stream << ", ";
    }
    result_stream << "\n";

    for (size_t i = 0; i < table_node->data.getSize(); ++i) {
        json entry = json::parse(table_node->data.get(i));

        // Проверка условий
        if (checkConditions(entry, conditions, logical_op)) {
            data_found = true;

            // Вывод указанных столбцов
            for (size_t j = 0; j < columns.getSize(); ++j) {
                if (columns.get(j) == "*") {
                    // Вывод всех столбцов
                    bool first = true;
                    for (auto it = entry.begin(); it != entry.end(); ++it) {
                        if (!first) result_stream << ", ";
                        result_stream << it.key() << ": \"" << it.value().get<string>() << "\"";
                        first = false;
                    }
                    break; // Завершаем, так как уже вывели все столбцы
                } else {
                    if (entry.contains(columns.get(j))) {
                        result_stream << columns.get(j) << ": \"" << entry[columns.get(j)].get<string>() << "\"";
                    } else {
                        result_stream << columns.get(j) << ": NULL";
                    }
                    if (j < columns.getSize() - 1) result_stream << ", ";
                }
            }
            result_stream << ";\n";
        }
    }

    if (!data_found) {
        result_stream << "No data found in the table: " << table << " with the specified conditions.\n";
    }
}

// Функция для обработки клиента
void handleClient(int client_socket, dbase& db) {
    char buffer[4096]; // Увеличен размер буфера для больших запросов
    while (true) {
        memset(buffer, 0, sizeof(buffer));
        int bytes_read = read(client_socket, buffer, sizeof(buffer) - 1);
        if (bytes_read <= 0) {
            break; // Выход из цикла при ошибке или закрытии соединения
        }
        string query(buffer);
        istringstream iss(query);
        string action;
        iss >> action;

        try {
            if (action == "INSERT") {
                string table;
                iss >> table;

                Array args;
                string arg;

                // Чтение всех оставшихся аргументов
                while (iss >> arg) {
                    args.addEnd(arg);
                }

                size_t expected_arg_count = db.getColumnCount(table);
                if (args.getSize() > expected_arg_count) {
                    string error_message = "Error: Too many arguments for INSERT command.";
                    send(client_socket, error_message.c_str(), error_message.size(), 0);
                    continue;
                } else if (args.getSize() < 2) {
                    string error_message = "Error: Not enough arguments for INSERT command.";
                    send(client_socket, error_message.c_str(), error_message.size(), 0);
                    continue;
                }

                json entry;
                entry["name"] = args.get(0);
                entry["age"] = args.get(1);

                if (args.getSize() > 2) {
                    entry["adress"] = args.get(2);
                } else {
                    entry["adress"] = ""; // Значение по умолчанию
                }

                if (args.getSize() > 3) {
                    entry["number"] = args.get(3);
                } else {
                    entry["number"] = ""; // Значение по умолчанию
                }

                insert(db, table, entry);
                string success_message = "Data successfully inserted.";
                send(client_socket, success_message.c_str(), success_message.size(), 0);

            }
            else if (action == "SELECT") {
                string select_part;
                // Чтение до 'FROM'
                getline(iss, select_part, 'F'); // 'F' из 'FROM'
                string from_str;
                iss >> from_str; // Остальные символы из 'FROM'

                if (from_str.substr(0, 3) != "ROM") { // Проверка на корректность 'FROM'
                    string error_message = "Error: Invalid SELECT syntax.";
                    send(client_socket, error_message.c_str(), error_message.size(), 0);
                    continue;
                }

                string table;
                iss >> table;

                // Проверка на наличие 'WHERE'
                string token;
                string where_clause;
                bool has_where = false;
                while (iss >> token) {
                    if (token == "WHERE") {
                        has_where = true;
                        break;
                    }
                }

                if (has_where) {
                    // Чтение остальной части запроса как WHERE
                    getline(iss, where_clause);
                }

                // Парсинг выбранных столбцов
                Array columns;
                // Удаление возможных пробелов и запятых
                size_t pos = select_part.find("SELECT");
                if (pos != string::npos) {
                    select_part = select_part.substr(pos + 6);
                }
                // Разделение столбцов по запятым
                size_t start = 0;
                while (start < select_part.length()) {
                    size_t comma = select_part.find(',', start);
                    if (comma != string::npos) {
                        string col = select_part.substr(start, comma - start);
                        // Удаление пробелов
                        size_t col_start = col.find_first_not_of(" \t");
                        size_t col_end = col.find_last_not_of(" \t");
                        if(col_start != string::npos && col_end != string::npos){
                            col = col.substr(col_start, col_end - col_start + 1);
                        }
                        columns.addEnd(col);
                        start = comma + 1;
                    }
                    else {
                        string col = select_part.substr(start);
                        // Удаление пробелов
                        size_t col_start = col.find_first_not_of(" \t");
                        size_t col_end = col.find_last_not_of(" \t");
                        if(col_start != string::npos && col_end != string::npos){
                            col = col.substr(col_start, col_end - col_start + 1);
                        }
                        if (!col.empty()) {
                            columns.addEnd(col);
                        }
                        break;
                    }
                }

                // Парсинг условий WHERE, если есть
                Array conditions;
                string logical_op;
                if (has_where) {
                    parseWhereClause(where_clause, conditions, logical_op);
                }

                // Выполнение выборки
                stringstream result_stream;
                if (conditions.getSize() == 0) {
                    // Выборка без условий
                    selectFromTable(db, table, columns, result_stream);
                }
                else {
                    // Выборка с условиями
                    selectFromTableWithConditions(db, table, columns, conditions, logical_op, result_stream);
                }

                string result = result_stream.str();
                send(client_socket, result.c_str(), result.size(), 0);

            }
            else if (action == "DELETE") {
                string from_word, table, column, value;
                iss >> from_word >> table >> column >> value;
                if (from_word != "FROM") {
                    string error_message = "Error: Invalid DELETE syntax.";
                    send(client_socket, error_message.c_str(), error_message.size(), 0);
                    continue;
                }
                deleteRow(db, column, value, table);
                string success_message = "Row deleted successfully.";
                send(client_socket, success_message.c_str(), success_message.size(), 0);
                
            }
            else {
                string error_message = "Unknown command: " + query;
                send(client_socket, error_message.c_str(), error_message.size(), 0);
            }
        }
        catch (const exception& e) {
            string error_message = "Error: " + string(e.what());
            send(client_socket, error_message.c_str(), error_message.size(), 0);
        }
    }
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
