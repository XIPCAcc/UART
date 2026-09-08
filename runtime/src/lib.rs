//! UINTR 用户态中断异步运行时
//!
//! 提供：
//! - 单线程 async 执行器（[executor]），基于 UMONITOR/UMWAIT 与用户态中断休眠
//! - 用户态中断（UINTR）基础设施：[uintr]（syscall/async_wait/connection/benchmark）、[uintr_core]
//! - 基于 UINTR 的传输通道：[shm]（共享内存 SPSC）、[pipe]（FIFO）
//! - 底层 FFI/事件循环支撑：[sys]、[reactor]、[signal]、[trace]、[error]
//!
//! 使用方式见各测试 crate（test-shm / test-pipe / test-pipe-latency / test-uintr）。

pub mod error;
pub mod executor;
pub mod pipe;
pub mod reactor;
pub mod shm;
pub mod signal;
pub mod sys;
pub mod trace;
pub mod uintr;
pub mod uintr_core;
