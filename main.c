#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <sys/fcntl.h>
#include <unistd.h>

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
 * 页类型
 */
typedef struct {
    int file_descriptor;
    uint32_t file_length;
    void* pages[TABLE_MAX_PAGES];
} Pager;

/**
 * 表类型
 */
typedef struct {
    uint32_t num_rows; // 行总数
//    void* pages[TABLE_MAX_PAGES]; // 所有的页
    Pager* pager; // 所有的页
} Table;

/**
 * Cursor抽象
 */
typedef struct {
    Table* table;
    uint32_t row_num;
    bool end_of_table; // 用来表示是否是最后一行
} Cursor;

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
 * 打开数据库文件
 * @param filename
 * @return
 */
Pager* pager_open(const char* filename) {
    // 打开文件
    int fd = open(filename,
                  O_RDWR |          // Read/Write模式
                        O_CREAT,    // 如果文件不存在就创建
                  S_IWUSR |         // 使用者(用户)写权限
                        S_IRUSR     // 使用者(用户)读权限
                  );
    if (fd == -1) {
        printf("不能打开文件\n");
        exit(EXIT_FAILURE);
    }

    // lseek(2) open(2) 括号里的2是对函数的分类，2代表是系统调用
    // 1是普通命令比如ls  3是库函数 比如printf 4是特殊文件，比如/dev下的各种设备文件
    // 获取文件的存储数据的长度
    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        // 初始化页
        pager->pages[i] = NULL;
    }
    return pager;
}

/**
 * 创建指向表结尾的cursor
 * @param table
 * @return Cursor实例
 */
Cursor* table_end(Table* table) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->end_of_table = true;
    return cursor;
}

/**
 * 创建指向表开头的cursor
 * @param table
 * @return Cursor实例
 */
Cursor* table_start(Table* table) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table = (table->num_rows == 0);
    return cursor;
}

/**
 * 表table的初始化函数
 * @return
 */
Table *db_open(const char *filename) {
    Pager* pager = pager_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;
    Table* table = (Table*) malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = num_rows;
//    table->num_rows = 0;
//    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
//        // 一开始pages都是NULL，只有在访问的时候才分配内存
//        table->pages[i] = NULL;
//    }
    return table;
}

/**
 * 将第n也写入文件
 * @param pager 页管理器
 * @param page_num  页编号
 * @param size 写入长度
 */
void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
    if (pager->pages[page_num] == NULL) {
        printf("视图保存空页\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("lseek报错\n");
        exit(EXIT_FAILURE);
    }

    // 写入文件中
    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);

    if (bytes_written == -1) {
        printf("写入失败\n");
        exit(EXIT_FAILURE);
    }
}

/**
 * 释放表内存的函数
 * @param table
 */
void db_close(Table* table) {
    Pager* pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE; // 满页数量

    for(uint32_t i = 0; i < num_full_pages; i++) {
        // 对于空page, 不操作
        if (pager->pages[i] == NULL) {
            continue;
        }
        // 持久化
        pager_flush(pager, i, PAGE_SIZE);
        // 释放page
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    // 存储非完整页(将来用BTree就不需要这一步操作了)
    uint32_t num_remain_rows = table->num_rows % ROWS_PER_PAGE;
    if (num_remain_rows > 0) {
        uint32_t page_num = num_full_pages;
        if (pager->pages[page_num] != NULL) {
            // 如果最后一页不为空, 做持久化
            pager_flush(pager, page_num, num_remain_rows * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
    }

    // 关闭文件
    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("关闭数据库失败\n");
        exit(EXIT_FAILURE);
    }
    // 最后对整个pages做一次清空
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    // 释放页管理器
    free(pager);
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
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        // 处理退出元指令
        db_close(table);
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
 * 根据页编号获取所在页地址
 * @param pager 页表数据结构
 * @param page_num 页编号
 * @return  所在页地址
 */
void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("页编号越界：%d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        // 如果是第一次使用改页，则分配内存空间
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        if (pager->file_length % PAGE_SIZE) {
            // 如果未除尽，说明还有一个未写完的页，把它加起来
            num_pages += 1;
        }

        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("读取文件错误\n");
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    return pager->pages[page_num];
}

/**
 * 根据Cursor实例获得当前行在内存中的偏移地址
 * @param cursor
 * @return  当前行的偏移地址
 */
void* cursor_value(Cursor* cursor) {
    // 根据行号获得当前页的编号
    uint32_t row_num = cursor->row_num;
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    // 获得所在页
//    void* page = table->pages[page_num];
//    if (page == NULL) {
//        // 只在我们视图访问页的时候分配页的内存
//        page = table->pages[page_num] = malloc(PAGE_SIZE);
//    }
    void* page = get_page(cursor->table->pager, page_num);
    uint32_t row_offset = row_num % ROWS_PER_PAGE; // 当前行之前的行数
    uint32_t byte_offset = row_offset * ROW_SIZE; // 当前行在当前页的偏移地址
    return page + byte_offset;
}

/**
 * 移动cursor
 * @param cursor
 */
void cursor_advance(Cursor* cursor) {
    cursor->row_num += 1;
    if (cursor->row_num >= cursor->table->num_rows) {
        cursor->end_of_table = true;
    }
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
        // 如果当前行数已经达到最大值，报满表错误
        return EXECUTE_TABLE_FULL;
    }
    Row* row_to_insert = &(statement->row_to_insert);
    // 创建Cursor实例
    Cursor* cursor = table_end(table);
    // 将statement中的row入表
    serialize_row(row_to_insert, cursor_value(cursor));
    // 表的行数加一
    table->num_rows += 1;

    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    Cursor* cursor = table_start(table);
    Row row;
//    for (uint32_t i = 0; i < table->num_rows; i++) {
//        // 把内存中的行读取到row
//        deserialize_row(row_slot(table, i), &row);
//        // 打印row
//        print_row(&row);
//    }
    while (!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }

    free(cursor);
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
int main(int argc, char* argv[]) {
    if (argc < 2) {
        // 如果参数小于2，说明没有提供数据库文件
        printf("必须提供一个数据库文件\n");
        exit(EXIT_FAILURE);
    }
    char* filename = argv[1];
    Table* table = db_open(filename);
    // 创建input_buffer
    InputBuffer* input_buffer = new_input_buffer();
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
            switch (do_meta_command(input_buffer, table)) {
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
