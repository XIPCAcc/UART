// 命令行参数解析：爬虫守护进程只保留监听地址一个配置项

pub struct Config {
    pub listen: String,
}

pub fn parse_args() -> Config {
    let args: Vec<String> = std::env::args().collect();
    let mut listen = String::from("127.0.0.1:9090");

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--listen" | "-l" => {
                i += 1;
                if i < args.len() {
                    listen = args[i].clone();
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

    Config { listen }
}

fn print_help() {
    println!(
        "crawler-daemon - Multi-core async crawler daemon\n\
         \n\
         USAGE:\n    crawler-daemon [OPTIONS]\n\
         \n\
         OPTIONS:\n    \
         -l, --listen <ADDR>      Listen address for crawl requests [default: 127.0.0.1:9090]\n\
         -h, --help               Print help\n\
         \n\
         EXAMPLES:\n    \
         # Listen on 0.0.0.0:9090, submit crawl: echo \"127.0.0.1:8080 /path\" | nc 127.0.0.1 9090\n    \
         crawler-daemon --listen 0.0.0.0:9090"
    );
}
