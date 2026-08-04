/**
 * UINTR Handler - 用户态中断处理程序
 * 
 * 这个文件提供了统一的中断处理程序实现
 */

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/syscall.h>
#include <stdbool.h>

// ============================================================================
// 数据结构
// ============================================================================

/**
 * UINTR栈帧结构（必须与内核一致）
 */
typedef struct {
    uint64_t rip;    // 中断返回地址
    uint64_t rflags; // 标志寄存器
    uint64_t rsp;    // 栈指针
} UintrFrame;

// ============================================================================
// 全局状态
// ============================================================================

/** 中断接收标志 */
volatile unsigned long uintr_received = 0;

// ============================================================================
// 外部函数声明
// ============================================================================

/** Rust回调函数，用于通知Rust代码中断已接收 */
extern void rust_interrupt_callback(const char* handler_name, uint64_t vector);

// ============================================================================
// 中断处理程序
// ============================================================================

/**
 * 统一的中断处理程序
 * 
 * 这个函数在收到用户态中断时被调用
 * 
 * @param _ui_frame UINTR栈帧（未使用）
 * @param vector 中断向量号（未使用，固定为0）
 */
void __attribute__ ((interrupt))
     __attribute__((target("general-regs-only", "inline-all-stringops")))
     ui_handler(UintrFrame* _ui_frame __attribute__((unused)), uint64_t vector __attribute__((unused))) {
    uintr_received = 1;
    rust_interrupt_callback(NULL, vector);
}

// ============================================================================
// 辅助函数
// ============================================================================

/**
 * 获取中断标志
 * 
 * @return 中断标志（0或1）
 */
int get_uintr_received(void) {
    return uintr_received;
}

/**
 * 设置中断标志
 * 
 * @param value 要设置的值（0或1）
 */
void set_uintr_received(int value) {
    uintr_received = value;
}
