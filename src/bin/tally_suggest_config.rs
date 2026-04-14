//! `tally suggest-config` — CLI that hits `/debug/config-recommendations`
//! and prints one-line-per-recommendation summaries grouped by pipeline name.
//!
//! Phase 25-02: zero new HTTP dependency. Uses a minimal blocking HTTP/1.1
//! GET client over std::net::TcpStream. This keeps the CLI tiny and means we
//! don't add `reqwest` / `ureq` just for one endpoint.

use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

fn print_usage() {
    eprintln!(
        "usage: tally_suggest_config [--addr URL] [--token TOKEN]\n\
         \n\
         Hits GET {{addr}}/debug/config-recommendations and prints a terse\n\
         summary. Default addr: http://localhost:6401.\n\
         Exit code is always 0 — recommendations are advisory."
    );
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let mut addr = "http://localhost:6401".to_string();
    let mut token: Option<String> = None;
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--addr" => {
                if i + 1 >= args.len() {
                    print_usage();
                    std::process::exit(2);
                }
                addr = args[i + 1].clone();
                i += 2;
            }
            "--token" => {
                if i + 1 >= args.len() {
                    print_usage();
                    std::process::exit(2);
                }
                token = Some(args[i + 1].clone());
                i += 2;
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {}", other);
                print_usage();
                std::process::exit(2);
            }
        }
    }

    // Parse URL: http://host:port
    let url = addr.trim();
    let rest = match url.strip_prefix("http://") {
        Some(r) => r,
        None => {
            eprintln!("only http:// addresses are supported, got {}", url);
            std::process::exit(2);
        }
    };
    let (host_port, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };
    let host = host_port.to_string();
    let port_stripped = if host_port.contains(':') {
        host_port.to_string()
    } else {
        format!("{}:80", host_port)
    };
    let full_path = if path == "/" {
        "/debug/config-recommendations".to_string()
    } else {
        format!("{}/debug/config-recommendations", path.trim_end_matches('/'))
    };

    // Connect + send request.
    let mut stream = match TcpStream::connect_timeout(
        &port_stripped.parse().unwrap_or_else(|_| {
            // If parse fails try a DNS-style resolve
            std::net::SocketAddr::from(([127, 0, 0, 1], 6401))
        }),
        Duration::from_secs(5),
    )
    .or_else(|_| TcpStream::connect(&port_stripped))
    {
        Ok(s) => s,
        Err(e) => {
            eprintln!("connection error to {}: {}", port_stripped, e);
            std::process::exit(1);
        }
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));

    let mut req = format!(
        "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nAccept: application/json\r\n",
        full_path, host
    );
    if let Some(tok) = &token {
        req.push_str(&format!("Authorization: Bearer {}\r\n", tok));
    }
    req.push_str("\r\n");
    if let Err(e) = stream.write_all(req.as_bytes()) {
        eprintln!("write error: {}", e);
        std::process::exit(1);
    }

    let mut buf = Vec::new();
    if let Err(e) = stream.read_to_end(&mut buf) {
        eprintln!("read error: {}", e);
        std::process::exit(1);
    }
    // Split headers/body on the first "\r\n\r\n".
    let body_start = match find_subslice(&buf, b"\r\n\r\n") {
        Some(idx) => idx + 4,
        None => {
            eprintln!("malformed response: no header/body separator");
            std::process::exit(1);
        }
    };
    let body = &buf[body_start..];
    // The response may be chunked. If so, decode.
    let body = if contains_header(&buf[..body_start], b"Transfer-Encoding: chunked")
        || contains_header(&buf[..body_start], b"transfer-encoding: chunked")
    {
        match dechunk(body) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("dechunk error: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        body.to_vec()
    };

    let parsed: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("response was not valid JSON: {}", e);
            eprintln!("body: {}", String::from_utf8_lossy(&body));
            std::process::exit(1);
        }
    };

    let recs = parsed
        .get("recommendations")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    if recs.is_empty() {
        println!("No recommendations — configuration looks healthy.");
        std::process::exit(0);
    }

    println!("# tally suggest-config — {} recommendation(s)", recs.len());
    for r in &recs {
        let knob = r.get("knob").and_then(|v| v.as_str()).unwrap_or("?");
        let current = r.get("current").and_then(|v| v.as_str()).unwrap_or("?");
        let suggested = r
            .get("suggested")
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        let reason = r.get("reason").and_then(|v| v.as_str()).unwrap_or("");
        println!("{}: '{}' → '{}'  ({})", knob, current, suggested, reason);
        if let Some(cp) = r.get("copy_paste").and_then(|v| v.as_str()) {
            println!("  {}", cp);
        }
    }
    std::process::exit(0);
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

fn contains_header(headers: &[u8], name: &[u8]) -> bool {
    find_subslice(headers, name).is_some()
}

fn dechunk(data: &[u8]) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < data.len() {
        // Find end of chunk size line.
        let nl = match data[i..].windows(2).position(|w| w == b"\r\n") {
            Some(n) => n,
            None => return Err("chunk header truncated".into()),
        };
        let size_str = std::str::from_utf8(&data[i..i + nl]).map_err(|e| e.to_string())?;
        let size = usize::from_str_radix(size_str.trim(), 16)
            .map_err(|e| format!("chunk size parse: {}", e))?;
        i += nl + 2;
        if size == 0 {
            break;
        }
        if i + size > data.len() {
            return Err("chunk body truncated".into());
        }
        out.extend_from_slice(&data[i..i + size]);
        i += size + 2; // skip trailing CRLF
    }
    Ok(out)
}
