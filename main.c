#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>

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

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;


/**
 * sql语句类型枚举
 */
typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

/**
 * 语句类型
 */
typedef struct {
    StatementType type;
} Statement;

InputBuffer* new_input_buffer() {
    InputBuffer * input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}


/**
 * 打印命令提示符
 */
void print_prompt() {
    printf("sql > ");
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

MetaCommandResult do_meta_command(InputBuffer* input_buffer) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        // 处理退出元指令
        close_input_buffer(input_buffer);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    // 识别插入
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        // 只比较前6个字符，使用strncmp
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    // 识别选择
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    // 如果到这里都没有识别出来，返回未识别成功
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement* statement) {
    // 分情况处理各种语句
    switch (statement->type) {
        case(STATEMENT_INSERT):
            printf("在这里处理insert语句\n");
            break;
        case(STATEMENT_SELECT):
            printf("在这里处理select语句\n");
            break;
    }
}

int main() {
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
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("未识别关键字: '%s'.\n", input_buffer->buffer);
                continue;
        }

        execute_statement(&statement);
        printf("执行完毕\n");
    }
}
