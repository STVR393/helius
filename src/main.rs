fn main() {
    let mut stdout = std::io::stdout();
    let mut stderr = std::io::stderr();

    match helius::run_app(std::env::args_os(), &mut stdout, &mut stderr) {
        Ok(()) => {}
        Err(helius::AppError::Clap(error)) => {
            let _ = error.print();
            std::process::exit(error.exit_code());
        }
        Err(error) => {
            eprintln!("{}", helius::format_error_message(&error.to_string()));
            std::process::exit(1);
        }
    }
}
