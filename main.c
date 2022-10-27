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

int main() {
    // 创建input_buffer
    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (strcmp(input_buffer->buffer, ".exit") == 0) {
            close_input_buffer(input_buffer);
            exit(EXIT_SUCCESS);
        } else {
            printf("未识别命令 '%s'\n", input_buffer->buffer);
        }
    }
}
