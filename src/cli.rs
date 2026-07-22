pub struct Config {
    pub port: String,
    pub baud: u32,
    pub mode: RunMode,
}

pub enum RunMode {
    Receiver,
    Sender { rows_a: u8, cols_a: u8, cols_b: u8, count: u32 },
}

pub fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut port = String::from("/dev/ttyS0");
    let mut baud = 115200u32;
    let mut mode = "receiver".to_string();
    let mut rows_a = 2u8;
    let mut cols_a = 3u8;
    let mut cols_b = 2u8;
    let mut count = 1u32;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                i += 1;
                if i < args.len() {
                    port = args[i].clone();
                }
            }
            "--baud" | "-b" => {
                i += 1;
                if i < args.len() {
                    baud = args[i].parse().unwrap_or(115200);
                }
            }
            "--mode" | "-m" => {
                i += 1;
                if i < args.len() {
                    mode = args[i].clone();
                }
            }
            "--rows-a" => {
                i += 1;
                if i < args.len() {
                    rows_a = args[i].parse().unwrap_or(2);
                }
            }
            "--cols-a" => {
                i += 1;
                if i < args.len() {
                    cols_a = args[i].parse().unwrap_or(3);
                }
            }
            "--cols-b" => {
                i += 1;
                if i < args.len() {
                    cols_b = args[i].parse().unwrap_or(2);
                }
            }
            "--count" | "-c" => {
                i += 1;
                if i < args.len() {
                    count = args[i].parse().unwrap_or(1);
                }
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    let run_mode = match mode.as_str() {
        "sender" => RunMode::Sender { rows_a, cols_a, cols_b, count },
        _ => RunMode::Receiver,
    };

    Config { port, baud, mode: run_mode }
}

fn print_help() {
    println!(
        "uart-matmul - Serial port matrix multiplication daemon\n\
         \n\
         USAGE:\n    uart-matmul [OPTIONS]\n\
         \n\
         OPTIONS:\n    \
         -p, --port <PORT>        Serial port device [default: /dev/ttyS0]\n    \
         -b, --baud <RATE>        Baud rate [default: 115200]\n    \
         -m, --mode <MODE>        Mode: receiver (default) or sender\n\
         \n\
         SENDER OPTIONS:\n    \
         --rows-a <N>             Rows of matrix A [default: 2]\n    \
         --cols-a <N>             Columns of A / rows of B [default: 3]\n    \
         --cols-b <N>             Columns of matrix B [default: 2]\n    \
         -c, --count <N>          Number of requests to send [default: 1]\n    \
         -h, --help               Print help\n\
         \n\
         EXAMPLES:\n    \
         # Receiver on ttyS0\n    \
         uart-matmul -p /dev/ttyS0\n\
         \n    \
         # Sender on ttyUSB0, 2x3 * 3x2, 5 requests\n    \
         uart-matmul -p /dev/ttyUSB0 --mode sender --rows-a 2 --cols-a 3 --cols-b 2 -c 5"
    );
}