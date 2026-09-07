use std::process::ExitCode;

fn main() -> ExitCode {
    match std::env::args().nth(1).as_deref() {
        Some("json") => print!("{}", hubuum::generate_operational_contract_json()),
        Some("metrics-markdown") => {
            print!("{}", hubuum::generate_operational_metrics_markdown());
        }
        _ => {
            eprintln!("usage: hubuum-operational-contracts <json|metrics-markdown>");
            return ExitCode::from(2);
        }
    }
    ExitCode::SUCCESS
}
