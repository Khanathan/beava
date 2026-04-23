//! Beava v2 server entry point.
//!
//! Plan 01 ships a placeholder that prints a banner and exits. Real arg parsing
//! lands in Plan 02, logging in Plan 03, HTTP in Plan 04.

fn main() {
    println!("{}", beava_server::banner());
    // Intentional: no args, no HTTP, no logging yet. Plans 02–04 wire those in.
}
