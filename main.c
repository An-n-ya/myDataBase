#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

/////////////////////////////////////////////// 宏
// 表的列
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

// 计算列属性所占空间
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

// 表属性
#define TABLE_MAX_PAGES 100

/////////////////////////////////////////////// 数据结构与枚举
/**
 * InputBuffer对getline里的参数进行包装
 */
typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

/**
 * 元指令枚举
 */
typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

/**
 * 解析sql语句结果枚举
 */
typedef enum {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID
} PrepareResult;

/**
 * sql语句执行结果枚举
 */
typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL
} ExecuteResult;


/**
 * sql语句类型枚举
 */
typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

/**
 * 表的行
 */
typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

/**
 * 语句类型
 */
typedef struct {
    StatementType type;
    Row row_to_insert;
} Statement;

/**
 * 表类型
 */
typedef struct {
    uint32_t num_rows; // 行总数
    void* pages[TABLE_MAX_PAGES]; // 所有的页
} Table;

//////////////////////////////////////////// 常量

// 记录每列占据的宽度
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
// 计算每列的偏移
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
// 计算一行的大小
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

// 表属性
const uint32_t PAGE_SIZE = 4096; // 每页大小为4096B
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE; // 计算每页平均可以容纳多少行
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES; // 计算整个表最多可以容纳多少行

//////////////////////////////////////////// 方法

/**
 * input_buffer的初始化函数
 * @return
 */
InputBuffer* new_input_buffer() {
    InputBuffer * input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

/**
 * 表table的初始化函数
 * @return
 */
Table* new_table() {
    Table* table = (Table*) malloc(sizeof(Table));
    table->num_rows = 0;
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        // 一开始pages都是NULL，只有在访问的时候才分配内存
        table->pages[i] = NULL;
    }
    return table;
}

/**
 * 释放表内存的函数
 * @param table
 */
void free_table(Table* table) {
    for(uint32_t i = 0; table->pages[i]; i++) {
        // 释放每个page
        free(table->pages[i]);
    }
    // 释放表
    free(table);
}


/**
 * 打印命令提示符
 */
void print_prompt() {
    printf("sql > ");
}

/**
 * 打印行
 * @param row
 */
void print_row(Row* row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

/**
 * 对getline函数进行封装，保存到input_buffer中去
 * @param input_buffer
 */
void read_input(InputBuffer* input_buffer) {
    // 从stdin中读取输入到input_buffer中
    ssize_t bytes_read = getline(
            &(input_buffer->buffer),
            &(input_buffer->buffer_length),
            stdin
            );

    // 如果读取失败，直接报错
    if (bytes_read <= 0) {
        printf("读取失败！\n");
        exit(EXIT_FAILURE);
    }

    // 忽略换行符
    input_buffer->buffer_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

/**
 * 释放input_buffer占用的资源
 * @param input_buffer
 */
void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

/**
 * 解析并执行元指令字符串
 * @param input_buffer
 * @return
 */
MetaCommandResult do_meta_command(InputBuffer* input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        // 处理退出元指令
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;

    // strtok用法，第一次使用的时候把str传进去，返回按分隔符分隔的第一个子字符串的地址
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL) {
        // 如果任何一个字段为空，则报错
        return PREPARE_SYNTAX_ERROR;
    }

    // id字符串转数字
    int id = atoi(id_string);
    // 检查id
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    // 检查字段的长度
    if (strlen(username) > COLUMN_USERNAME_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    // 运行到这里说明一切顺利，将字符串拷贝进statement
    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}


/**
 * 解析sql语句
 * @param input_buffer
 * @param statement
 * @return
 */
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    // 识别插入
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        // 只比较前6个字符，使用strncmp
        statement->type = STATEMENT_INSERT;
        return prepare_insert(input_buffer, statement);
    }
    // 识别选择
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    // 如果到这里都没有识别出来，返回未识别成功
    return PREPARE_UNRECOGNIZED_STATEMENT;
}



/**
 * 将当前行放入内存中
 * @param source 当前行的地址
 * @param destination 目标内存的地址
 */
void serialize_row(Row* source, void* destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

/**
 * 将内存中的行放入目标位置
 * @param source 内存中行的地址
 * @param destination 目标位置
 */
void deserialize_row(void* source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source+ USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

/**
 * 根据行号获得当前行在内存中的偏移地址
 * @param table  表
 * @param row_num 行号
 * @return  当前行的偏移地址
 */
void* row_slot(Table* table, uint32_t row_num) {
    // 根据行号获得当前页的编号
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    // 获得所在页
    void* page = table->pages[page_num];
    if (page == NULL) {
        // 只在我们视图访问页的时候分配页的内存
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE; // 当前行之前的行数
    uint32_t byte_offset = row_offset * ROW_SIZE; // 当前行在当前页的偏移地址
    return page + byte_offset;
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        // 如果当前行数已经达到最大值，报满表错误
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert = &(statement->row_to_insert);
    // 将statement中的row入表
    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    // 表的行数加一
    table->num_rows += 1;
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    Row row;
    for (uint32_t i = 0; i < table->num_rows; i++) {
        // 把内存中的行读取到row
        deserialize_row(row_slot(table, i), &row);
        // 打印row
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

/**
 * 执行sql语句
 * @param statement 待执行的语句
 * @param table 当前表
 * @return 执行结果
 */
ExecuteResult execute_statement(Statement* statement, Table* table) {
    // 分情况处理各种语句
    switch (statement->type) {
        case(STATEMENT_INSERT):
            return execute_insert(statement, table);
        case(STATEMENT_SELECT):
            return execute_select(statement, table);
    }
}



///////////////////////////////////////////////////  入口函数
int main() {
    // 创建input_buffer
    InputBuffer* input_buffer = new_input_buffer();
    Table* table = new_table();
    while (true) {
        print_prompt();
        read_input(input_buffer);

//        if (strcmp(input_buffer->buffer, ".exit") == 0) {
//            close_input_buffer(input_buffer);
//            exit(EXIT_SUCCESS);
//        } else {
//            printf("未识别命令 '%s'\n", input_buffer->buffer);
//        }
        // 如果指令以 . 开头，说明是元指令，使用 do_meta_command指令处理
        // 否则当做sql语句处理
        if (input_buffer->buffer[0] == '.') {

            // .开头的元指令
            switch (do_meta_command(input_buffer)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("未识别命令 '%s'\n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("语法错误，不能解析语句\n");
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("输入参数过长\n");
                continue;
            case (PREPARE_NEGATIVE_ID):
                printf("ID必须为非负数\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("未识别关键字: '%s'.\n", input_buffer->buffer);
                continue;
        }

        switch (execute_statement(&statement, table)) {
            case(EXECUTE_SUCCESS):
                printf("执行完毕\n");
                break;
            case(EXECUTE_TABLE_FULL):
                printf("错误：表已经满了\n");
                break;
        }
    }
}
