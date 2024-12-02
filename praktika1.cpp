#include <iostream>
#include <fstream>
#include <sys/stat.h> // Для mkdir
#include "json.hpp" // Библиотека для работы с JSON
#include <sstream> // Для istringstream

using namespace std;
using json = nlohmann::json;

template<typename T1, typename T2>
struct Pars {
    T1 first;
    T2 second;

    Pars() : first(T1()), second(T2()) {} // Конструктор по умолчанию
    Pars(const T1& f, const T2& s) : first(f), second(s) {}
};

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

struct Node {
    string name;
    Array data;
    Node* next;

    Node(const string& name) : name(name), next(nullptr) {}
};
template <typename T>
struct NodeS {
    T data;
    NodeS* next;

    NodeS(T data) : data(data), next(nullptr) {}
};

template <typename T>
struct Spisok {
    NodeS<T>* head;
    NodeS<T>* tail;
    size_t size;

    Spisok() : head(nullptr), tail(nullptr), size(0) {}

    ~Spisok() {
        while (head) {
            NodeS<T>* temp = head;
            head = head->next;
            delete temp;
        }
    }
    bool isEmpty() const {
        return head == nullptr; // Если голова равна nullptr, список пуст
    }
    void append(const T& data) {
        NodeS<T>* newNode = new NodeS<T>(data);
        if (isEmpty()) {
            head = newNode;
        } else {
            NodeS<T>* current = head;
            while (current->next != nullptr) {
                current = current->next;
            }
            current->next = newNode;
        }
    }
    void addEnd(const T& value) {
        NodeS<T>* newNode = new NodeS<T>(value);
        if (!head) {
            head = newNode;
            tail = newNode;
        } else {
            tail->next = newNode;
            tail = newNode;
        }
        size++;
    }

    size_t getSize() const {
        return size;
    }
};
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
            // Формируем путь к файлу CSV
            string filename = schema_name + "/" + table + "/1.csv"; // Изменено на '/'
            ifstream file(filename);
            if (file) {
                string header;
                if (getline(file, header)) {
                    // Считаем количество запятых в заголовке
                    size_t comma_count = std::count(header.begin(), header.end(), ',');
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
                        line.erase(0, line.find_first_not_of(" \t"));
                        line.erase(line.find_last_not_of(" \t") + 1);

                        if (is_header) {
                            is_header = false;
                            continue;
                        }

                        istringstream iss(line);
                        Array fields;
                        string field;

                        while (getline(iss, field, ',')) {
                            field.erase(0, field.find_first_not_of(" \t"));
                            field.erase(field.find_last_not_of(" \t") + 1);
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

void createDirectories(dbase& db, const json& structure);
void lockPrimaryKey(dbase& db) {
    try {
        string pk_filename = db.schema_name + "/table_pk_sequence.txt";
        ofstream pk_file(pk_filename);
        if (pk_file) {
            pk_file << db.current_pk << "\nlocked";
        } else {
            throw runtime_error("Failed to lock primary key file: " + pk_filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

void unlockPrimaryKey(dbase& db) {
    try {
        string pk_filename = db.schema_name + "/table_pk_sequence.txt";
        ofstream pk_file(pk_filename);
        if (pk_file) {
            pk_file << db.current_pk << "\nunlocked";
        } else {
            throw runtime_error("Failed to unlock primary key file: " + pk_filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

void loadSchema(dbase& db, const string& schema_file) {
    try {
        ifstream file(schema_file);
        if (file) {
            json schema;
            file >> schema;
            db.schema_name = schema["name"];
            createDirectories(db, schema["structure"]);
            for (const auto& table : schema["structure"].items()) {
                db.addNode(table.key());
            }
        } else {
            throw runtime_error("Failed to open schema file.");
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

void initializePrimaryKey(dbase& db) {
    try {
        string pk_filename = db.schema_name + "/table_pk_sequence.txt";
        ifstream pk_file(pk_filename);
        
        if (pk_file) {
            pk_file >> db.current_pk;
        } else {
            db.current_pk = 0;
            ofstream pk_file_out(pk_filename);
            if (pk_file_out) {
                pk_file_out << db.current_pk << "\nunlocked";
            } else {
                throw runtime_error("Failed to create file: " + pk_filename);
            }
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

void updatePrimaryKey(dbase& db) {
    try {
        string pk_filename = db.schema_name + "/table_pk_sequence.txt";

        ifstream pk_file(pk_filename);
        if (pk_file) {
            pk_file >> db.current_pk;
        } else {
            db.current_pk = 0;
        }
        pk_file.close();

        db.current_pk++;

        ofstream pk_file_out(pk_filename);
        if (pk_file_out) {
            pk_file_out << db.current_pk << "\nlocked";
        } else {
            throw runtime_error("Failed to open file for updating: " + pk_filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

void rewriteCSV(dbase& db, const string& table);

void createDirectories(dbase& db, const json& structure) {
    try {
        // Создаем директорию для схемы
        if (mkdir(db.schema_name.c_str(), 0777) && errno != EEXIST) {
            throw runtime_error("Failed to create directory: " + db.schema_name);
        }

        for (const auto& table : structure.items()) {
            string table_name = table.key();
            string table_path = db.schema_name + "/" + table_name;

            // Создаем директорию для таблицы
            if (mkdir(table_path.c_str(), 0777) && errno != EEXIST) {
                throw runtime_error("Failed to create directory: " + table_path);
            }
            db.filename = table_path + "/1.csv";

            ifstream check_file(db.filename);
            if (!check_file) {
                ofstream file(db.filename);
                if (file.is_open()) {
                    auto& columns = table.value();
                    for (size_t i = 0; i < columns.size(); ++i) {
                        file << setw(10) << left << columns[i].get<string>() << (i < columns.size() - 1 ? ", " : "");
                    }
                    file << "\n";
                    file.close();
                }
            }

            initializePrimaryKey(db);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

bool applyFilters(const string& table, const json& entry, const Spisok<Pars<string, string>>& filters) {
    
    if (filters.head == nullptr) {
        return false; // Если список пуст, возвращаем false
    }
    if (table == "table1"){
        const string& filter_column = filters.head->data.first; // Получаем ключ фильтра
        const string& filter_value = filters.head->data.second; // Получаем значение фильтра

        // Проверяем, есть ли колонка в записи и совпадает ли значение
        if (!entry.contains(filter_column) || entry[filter_column].get<string>() != filter_value) {
            return false; // Если не совпадает, возвращаем false
        }
    }   
    else{

        NodeS<Pars<string, string>>* current = filters.head;
        current = current->next;
        const string& filter_column = current->data.first; // Получаем ключ фильтра
        const string& filter_value = current->data.second; // Получаем значение фильтра
        if (!entry.contains(filter_column) || entry[filter_column].get<string>() != filter_value) {
            return false; // Если не совпадает, возвращаем false
        }
    }
    return true; // Если фильтр прошёл, возвращаем true
}

bool applyFilter(const json& entry, const Spisok<Pars<string, string>>& filters) {
    
    if (filters.head == nullptr) {
        return false; // Если список пуст, возвращаем false
    }
        const string& filter_column = filters.head->data.first; // Получаем ключ фильтра
        const string& filter_value = filters.head->data.second; // Получаем значение фильтра

        // Проверяем, есть ли колонка в записи и совпадает ли значение
        if (!entry.contains(filter_column) || entry[filter_column].get<string>() != filter_value) {
            return false; // Если не совпадает, возвращаем false
        } 

    return true; // Если фильтр прошёл, возвращаем true
}



void selectFromMultipleTables(dbase& db, const string& column,const string& column2, const string& table1,const string& table2, Spisok<Pars<string, string>> filters, int filter_count,const string& WHERE ,const string& filter_type,const string& tablef) {

    string column1 = column; 

    bool data_found = false;
    if (column2!="") {         
        Node* table_node1 = db.findNode(table1);
        Node* table_node2 = db.findNode(table2);
        if (!table_node1 || !table_node2) {
            cout << "One or both tables not found: " << table1 << ", " << table2 << endl;
            return;
        }
        if (WHERE == "WHERE"){
            if( filter_type != ""){
                for (size_t i = 0; i < table_node1->data.getSize(); ++i) {
                    json entry1 = json::parse(table_node1->data.get(i));

                    for (size_t j = 0; j < table_node2->data.getSize(); ++j) {
                        json entry2 = json::parse(table_node2->data.get(j));

                        if ((filter_type == "AND" && applyFilters(table1, entry1, filters) && applyFilters(table2, entry2, filters)) ||
                        (filter_type == "OR" && (applyFilters(table1, entry1, filters) || applyFilters(table2, entry2, filters)))) {
                    
                            if (entry1.contains(column1) && entry2.contains(column2)) {
                                cout << entry1[column1].get<string>() << ", " << entry2[column2].get<string>() << endl;
                                data_found = true;
                            }
                        }
                    }
                }
            }
            else{
                for (size_t i = 0; i < table_node1->data.getSize(); ++i) {
                    json entry1 = json::parse(table_node1->data.get(i));

                    for (size_t j = 0; j < table_node2->data.getSize(); ++j) {
                        json entry2 = json::parse(table_node2->data.get(j));
                        if(tablef=="table1"){
                            if (applyFilter(entry1, filters)) {
                               
                                if (entry1.contains(column1) && entry2.contains(column2)) {
                                    cout << entry1[column1].get<string>() << ", " << entry2[column2].get<string>() << endl;
                                    data_found = true;
                                }   
                            }
                        } else{
                            if (applyFilter(entry2, filters)) {
                               
                                if (entry1.contains(column1) && entry2.contains(column2)) {
                                    cout << entry1[column1].get<string>() << ", " << entry2[column2].get<string>() << endl;
                                    data_found = true;
                                }   
                            }
                        }
                    }
                }
            }
        } else{
            for (size_t i = 0; i < table_node1->data.getSize(); ++i) {
                json entry1 = json::parse(table_node1->data.get(i));

                for (size_t j = 0; j < table_node2->data.getSize(); ++j) {
                    json entry2 = json::parse(table_node2->data.get(j));

                    if (entry1.contains(column1) && entry2.contains(column2)) {
                        cout << entry1[column1].get<string>() << ", " << entry2[column2].get<string>() << endl;
                        data_found = true;
                    }
                }
            }
        }
    } else {
            cout << "Error: Missing column names for CROSS JOIN." << endl;
            return;
    }
    

    if (!data_found) {
        cout << "No data found in the cross join of " << table1 << " and " << table2 << endl;
    }
}

void selectFromTable(dbase& db, const string& table, Spisok<Pars<string, string>> filters, int filter_count, const string& filter_type) {
    Node* table_node = db.findNode(table);
    
    if (!table_node) {
        cout << "Table not found: " << table << endl;
        return;
    }

    bool data_found = false;
    for (size_t i = 0; i < table_node->data.getSize(); ++i) {
        json entry = json::parse(table_node->data.get(i));
        data_found = true; // We found at least one entry

        // Print the entry in the desired format
        cout << "name: \"" << entry["name"].get<string>() << "\", "
             << "age: \"" << entry["age"].get<string>() << "\", "
             << "adress: \"" << entry["adress"].get<string>() << "\", "
             << "number: \"" << entry["number"].get<string>() << "\";" << endl;
    }

    if (!data_found) {
        cout << "No data found in the table: " << table << endl;
    }
}

void saveSingleEntryToCSV(dbase& db, const string& table, const json& entry) {
    try {
        string filename = db.schema_name + "/" + table + "/1.csv"; 
        ofstream file(filename, ios::app);
        if (file) {
            // Проверяем, сколько полей есть в JSON-объекте
            if (entry.contains("name") && entry.contains("age")) {
                file << setw(10) << left << entry["name"].get<string>() << ", "
                     << setw(10) << left << entry["age"]
                     << ", " << setw(10) << left << entry["adress"].get<string>() << ", "
                    << setw(10) << left << entry["number"].get<string>();

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

void insert(dbase& db, const string& table, json entry) {
    Node* table_node = db.findNode(table);
    if (table_node) {
        // Если количество аргументов совпадает с количеством колонок, продолжаем
        updatePrimaryKey(db); 
        entry["id"] = db.current_pk; 

        table_node->data.addEnd(entry.dump());

        saveSingleEntryToCSV(db, table, entry);
    } else {
        cout << "Table not found: " << table << endl;
    }
}

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
            rewriteCSV(db, table);
        } else {
            cout << "Row with " << column << " = " << value << " not found in " << table << endl;
        }
    } else {
        cout << "Table not found: " << table << endl;
    }
}

void rewriteCSV(dbase& db, const string& table) {
    try {
        db.filename = db.schema_name + "/" + table + "/1.csv"; 
        ofstream file(db.filename); 

        if (file) {
            Node* table_node = db.findNode(table);
            if (table_node) {
                json columns = {"name", "age", "adress", "number"};

                for (const auto& column : columns) {
                    file << setw(10) << left << column.get<string>() << (column != columns.back() ? ", " : "");
                }
                file << "\n"; 

                for (size_t i = 0; i < table_node->data.getSize(); ++i) {
                    json entry = json::parse(table_node->data.get(i));
                    for (const auto& column : columns) {
                        file << setw(10) << left << entry[column.get<string>()].get<string>() << (column != columns.back() ? ", " : "");
                    }
                    file << "\n"; 
                }
            }
            file.close();
        } else {
            throw runtime_error("Failed to open data file for rewriting: " + db.filename);
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }
}

int main(int argc, char* argv[]) {
    dbase db;
    try {
        loadSchema(db, "schema.json");
        db.load();

        if (argc > 2 && string(argv[1]) == "--query") {
            string query = argv[2];
            istringstream iss(query);
            string action;
            iss >> action;

            try {
                if (action == "INSERT") {
                    string table;
                    iss >> table;

                    Array args; // Используем новый массив
                    string arg;

                    // Чтение всех оставшихся аргументов
                    while (iss >> arg) {
                        args.addEnd(arg);
                    }

                    // Получаем ожидаемое количество аргументов
                    size_t expected_arg_count = db.getColumnCount(table);
                    if (args.getSize() > expected_arg_count) {
                        cout << "Error: Too many arguments (" << args.getSize() << ") for INSERT command." << endl;
                        return 1;
                    } else if (args.getSize() < 2) { 
                        cout << "Error: Not enough arguments (" << args.getSize() << ") for INSERT command." << endl;
                        return 1;
                    }

                    // Создание JSON-объекта из аргументов
                    json entry = {
                        {"name", args.get(0)},
                        {"age", stoi(args.get(1))}
                    };

                    // Добавляем дополнительные поля, если они есть
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
                } else if (action == "SELECT") {
                    string column,column2, from, tables;
                    iss >> from >> tables;

                    if (tables == "tables:") {
                        string table1, and_word, table2, WHERE,filter_column1,filter_value1,filter_value2, OPER, filter_column2,tablef;
                        iss >> table1 >> column >> and_word >> table2>> column2 >>WHERE >> tablef >> filter_column1>> filter_value1>> OPER>> table2>>filter_column2>>filter_value2 ;
                        Spisok<Pars<string, string>> filters;
                        int filter_count = 0;
                        filters.addEnd(Pars<string, string>(filter_column1, filter_value1));
                        filters.addEnd(Pars<string, string>(filter_column2, filter_value2));

                        selectFromMultipleTables(db, column,column2, table1, table2, filters, 0,WHERE,OPER,tablef);

                    } else if (tables != "tables:"){
                        string table;
                        table = tables;
                        selectFromTable(db, table, {}, 0, "AND");

                    } else {
                        throw runtime_error("Invalid query format.");
                    }
                } else if (action == "DELETE") {
                    string column, from, value, table;
                    iss >> from >> table >> column >> value;
                    deleteRow(db, column, value, table);
                } else {
                    throw runtime_error("Unknown command: " + query);
                }
            } catch (const exception& e) {
                cout << "Error: " << e.what() << endl;
            }
        } else {
            cout << "Usage: " << argv[0] << " --query '<your query>'" << endl;
        }
    } catch (const exception& e) {
        cout << "Error: " << e.what() << endl;
    }

    return 0;
}
