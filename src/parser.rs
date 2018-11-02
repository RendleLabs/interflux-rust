use std::str;
use nom::*;

named!( terminator<char>, one_of!(&b" ,\n"[..]));
named!( until_terminator, take_until_either!(" ,\n") );// take_till!(|ch| ch == b',' || ch == b' ' || ch == b'\n') );

named!( delimiter_to_equal_sign,
    preceded!(
        one_of!(&b" ,"[..]),
        take_until_and_consume!("=")
    )
);

named!( space_to_equal_sign,
    preceded!(
        tag!(" "),
        take_until_and_consume!("=")
    )
);

named!( field_name,
    take_until_and_consume!("=")
);

named!( field_value,
    take_until_either!(", \n")
);

named!( comma_to_equal_sign,
    preceded!(
        tag!(","),
        take_until_and_consume!("=")
    )
);

named!( measurement, take_until_either!(" ,"));

named!( tag<&[u8], (&[u8], &[u8])>,
    pair!(
        comma_to_equal_sign,
        until_terminator
    )
);

named!( tags<&[u8], Vec<(&[u8],&[u8])> >,
    many0!( tag )
);

named!( first_field<&[u8], (&[u8], &[u8])>,
    pair!(
        space_to_equal_sign,
        until_terminator
    )
);

named!( other_field<&[u8], (&[u8], &[u8])>,
    pair!(
        comma_to_equal_sign,
        until_terminator
    )
);

//named!( field<&[u8], (&[u8], &[u8])>, alt!(first_field | other_field));
named!( field<&[u8], (&[u8], &[u8])>,
    preceded!(
        opt!(tag!(",")),
        pair!(
            field_name,
            field_value
        )
    )
);

named!( fields<&[u8], Vec<(&[u8],&[u8])> >,
    do_parse!(
        first: first_field >>
        others: many0!( other_field ) >>
        ( combine_fields(first, others) )
    )
);

named!( timestamp<&[u8], Option<&[u8]> >,
    opt!(
        preceded!(tag!(" "), digit0)
    )
);

fn combine_fields<'a>(first: (&'a [u8], &'a [u8]), others: Vec<(&'a [u8],&'a[u8])>) -> Vec<(&'a [u8],&'a [u8])> {
    let mut v: Vec<(&[u8],&[u8])> = Vec::with_capacity(others.len() + 1);
    v.push(first);
    v.extend(&others);
    v
}

fn get_timestamp(input: &[u8]) -> IResult<&[u8], Option<&[u8]>> {
    if input.len() == 0 {
        return Ok((input, None));
    }
    if input[0] == b'\n' {
        return Ok((&input[1..], None))
    }

    let r = timestamp(input);
    match r {
        Ok((remaining, value)) => Ok((remaining, value)),
        Err(Err::Incomplete(needed)) => Ok((input, Some(&input[1..]))),
        Err(e) => Err(e),
    }
}

pub struct Metric<'a> {
    measurement: String,
    tags: Option<Vec<(&'a[u8], &'a[u8])>>,
    fields: Option<Vec<(String, String)>>,
    timestamp: Option<u64>,
}

pub struct Line {
    measurement: String,
    tags: Option<Vec<(String, String)>>,
    fields: Option<Vec<(String, String)>>,
    timestamp: Option<u64>,
}

pub fn parse(stream: &[u8]) -> Option<Line> {
    let (r, s) = until_terminator(stream).unwrap();
    let (r, t) = until_terminator(r).unwrap();
    let x = str::from_utf8(s).unwrap();
    Some(Line {
        measurement: String::from(x),
        tags: None,
        fields: None,
        timestamp: None,
    })
}

#[test]
fn check_until_terminator_with_comma() {
    let t = b"requests,method=GET";
    let r = until_terminator(t);
    match r {
        Ok((remaining, value)) => {
            assert_eq!(value, b"requests");
            assert_eq!(remaining[0], b',');
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_until_terminator_with_space() {
    let t = b"requests count=1i";
    let r = until_terminator(t);
    match r {
        Ok((remaining, value)) => {
            assert_eq!(value, b"requests");
            assert_eq!(remaining[0], b' ');
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_until_terminator_with_newline() {
    let t = b"requests\n";
    let r = until_terminator(t);
    match r {
        Ok((remaining, value)) => {
            assert_eq!(value, b"requests");
            assert_eq!(remaining[0], b'\n');
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_delimiter_to_equal_sign() {
    let t = b",method=GET\n";
    let r = delimiter_to_equal_sign(t);
    match r {
        Ok((remaining, value)) => {
            assert_eq!(value, b"method");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_field_with_trailing_space() {
    let t = b"method=GET ";
    let r = field(t);
    match r {
        Ok((remaining, (name, value))) => {
            assert_eq!(name, b"method");
            assert_eq!(value, b"GET");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_field_with_trailing_comma() {
    let t = b"method=GET,";
    let r = field(t);
    match r {
        Ok((remaining, (name, value))) => {
            assert_eq!(name, b"method");
            assert_eq!(value, b"GET");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_field_with_leading_comma_and_trailing_comma() {
    let t = b",method=GET,";
    let r = field(t);
    match r {
        Ok((remaining, (name, value))) => {
            assert_eq!(name, b"method");
            assert_eq!(value, b"GET");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

//#[test]
//fn check_field_with_preceding_space_and_trailing_space() {
//    let t = b" method=GET ";
//    let r = field(t);
//    match r {
//        Ok((remaining, (name, value))) => {
//            assert_eq!(name, b"method");
//            assert_eq!(value, b"GET");
//        }
//        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
//        Err(Err::Error(e)) => panic!("Error: {:?}", e),
//        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
//    };
//}
//
//#[test]
//fn check_field_with_preceding_space_and_trailing_comma() {
//    let t = b" method=GET,";
//    let r = field(t);
//    match r {
//        Ok((remaining, (name, value))) => {
//            assert_eq!(name, b"method");
//            assert_eq!(value, b"GET");
//        }
//        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
//        Err(Err::Error(e)) => panic!("Error: {:?}", e),
//        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
//    };
//}

#[test]
fn check_measurement() {
    let t = b"requests count=1 1234567890\n";
    let r = measurement(t);
    match r {
        Ok((remaining, measurement)) => {
            assert_eq!(measurement, b"requests");
            assert_eq!(remaining, b" count=1 1234567890\n");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_timestamp_with_newline() {
    let t = b" 1234567890\n";
    let r = get_timestamp(t);
    match r {
        Ok((remaining, timestamp)) => {
            assert_eq!(timestamp.unwrap(), b"1234567890");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_timestamp_with_no_newline() {
    let t = b" 1234567890";
    let r = get_timestamp(t);
    match r {
        Ok((remaining, timestamp)) => {
            assert_eq!(timestamp.unwrap(), b"1234567890");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

//#[test]
//fn check_line_with_timestamp_and_newline() {
//    let t = b"requests count=1 1234567890\n";
//    let r = line(t);
//    match r {
//        Ok((remaining, (measurement, fields, timestamp))) => {
//            assert_eq!(measurement, b"requests");
//            assert_eq!(fields, b"count=1");
//            assert_eq!(timestamp.unwrap(), b"1234567890");
//        }
//        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
//        Err(Err::Error(e)) => panic!("Error: {:?}", e),
//        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
//    };
//}
//
//#[test]
//fn check_line_with_timestamp_and_no_newline() {
//    let t = b"requests count=1 1234567890";
//    let r = line(t);
//    match r {
//        Ok((remaining, (measurement, fields, timestamp))) => {
//            assert_eq!(measurement, b"requests");
//            assert_eq!(fields, b"count=1");
//            assert_eq!(timestamp.unwrap(), b"1234567890");
//        }
//        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
//        Err(Err::Error(e)) => panic!("Error: {:?}", e),
//        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
//    };
//}

#[test]
fn check_tag_with_trailing_comma() {
    let t = b",method=GET,blah";
    let r = tag(t);
    match r {
        Ok((remaining, (key, value))) => {
            assert_eq!(key, b"method");
            assert_eq!(value, b"GET");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_tag_with_trailing_space() {
    let t = b",method=GET ";
    let r = tag(t);
    match r {
        Ok((remaining, (key, value))) => {
            assert_eq!(key, b"method");
            assert_eq!(value, b"GET");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_tags() {
    let t = b",method=GET,host=foo ";
    let r = tags(t);
    match r {
        Ok((remaining, vec)) => {
            assert_eq!(vec.len(), 2);
            let (key, value) = vec[0];
            assert_eq!(key, b"method");
            assert_eq!(value, b"GET");
            let (key, value) = vec[1];
            assert_eq!(key, b"host");
            assert_eq!(value, b"foo");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_fields() {
    let t = b" count=1,duration=5 ";
    let r = fields(t);
    match r {
        Ok((remaining, vec)) => {
            assert_eq!(vec.len(), 2);
            let (key, value) = vec[0];
            assert_eq!(key, b"count");
            assert_eq!(value, b"1");
            let (key, value) = vec[1];
            assert_eq!(key, b"duration");
            assert_eq!(value, b"5");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

//#[test]
//fn check_measurement_and_tags() {
//    let t = b"requests,method=GET,host=foo ";
//    let r = measurement_and_tags(t);
//    match r {
//        Ok((remaining, (measurement, vec))) => {
//            assert_eq!(measurement, b"requests");
//            assert_eq!(vec.len(), 2);
//            let (key, value) = vec[0];
//            assert_eq!(key, b"method");
//            assert_eq!(value, b"GET");
//            let (key, value) = vec[1];
//            assert_eq!(key, b"host");
//            assert_eq!(value, b"foo");
//        }
//        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
//        Err(Err::Error(e)) => panic!("Error: {:?}", e),
//        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
//    };
//}
