fn main() {
    println!("cargo:rerun-if-changed=../handler.c");
    cc::Build::new()
        .file("../handler.c")
        .flag("-muintr")
        .flag("-O3")
        .flag("-std=gnu11")
        .compile("handler");
}
