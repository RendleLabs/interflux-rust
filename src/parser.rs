use std::collections::HashMap;

named!( until_comma_or_space, take_till!(|ch| ch == b',' || ch == b' ') );

pub struct Line {
    measurement: str,
    tags: Option<HashMap<str,str>>,
    fields: Option<HashMap<str, str>>,
    timestamp: Option<u64>
}

pub fn parse(stream: &[u8]) -> Line {
    let m = match until_comma_or_space(stream) {
        Ok((r,v)) => v,
        Err(e) => []
    };
    Line {
        measurement: String::from_utf8(m),
        tags: None,
        fields: None,
        timestamp: None
    }
}
