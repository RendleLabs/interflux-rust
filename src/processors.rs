use bytes::buf::BufMut;
use bytes::BytesMut;
use std::collections::HashSet;
use std::fmt::Write;
use std::str;

use crate::parser::*;

pub struct MetricProcessor {
    pub tags: HashSet<String>,
}

impl MetricProcessor {
    pub fn new(strings: Vec<String>) -> MetricProcessor {
        let mut tags: HashSet<String> = HashSet::with_capacity(strings.len());
        for string in strings.iter() {
            tags.insert(string.clone());
        }
        MetricProcessor { tags }
    }

    pub fn process(&self, name: &str, data: &[u8]) -> usize {
        let mut buf = BytesMut::with_capacity(1024);
        let mut src = data;
        match buf.write_str(name) {
            Ok(_) => {},
            Err(_) => {
                return 0;
            },
        }
        match parse_tags(src) {
            Some((remaining, tags)) => {
                for (tag, value) in tags {
                    let stag = str::from_utf8(tag).unwrap();
                    println!("Tag: {}", stag);
                    if !self.tags.contains(stag) {
                        buf.put(b',');
                        buf.extend_from_slice(tag);
                        buf.put(b'=');
                        buf.extend_from_slice(value);
                    }
                }
                src = remaining;
            }
            None => (),
        }
        match parse_fields(src) {
            Some((remaining, fields)) => {
                let mut delimit = b' ';
                for (field, value) in fields {
                    buf.put(delimit);
                    buf.extend_from_slice(field);
                    buf.put(b'=');
                    buf.extend_from_slice(value);
                    delimit = b',';
                }
                src = remaining;
            }
            None => (),
        }
        match parse_timestamp(src) {
            Some((_, timestamp)) => {
                buf.put(b' ');
                buf.extend_from_slice(timestamp);
            }
            None => (),
        }
        buf.put(b'\n');
        match String::from_utf8(buf.to_vec()) {
            Ok(s) => {
                println!("Processed: '{}'", s);
            }
            Err(_) => (),
        }
        1
    }
}
