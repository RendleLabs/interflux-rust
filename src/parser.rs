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

named!( metric<&[u8], Metric>,
    do_parse!(
        m: measurement >>
        t: tags >>
        f: fields >>
        ts: timestamp >>
        (
            Metric {
                measurement: m,
                tags: t,
                fields: f,
                timestamp: ts,
            }
        )
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
        Err(Err::Incomplete(_)) => Ok((input, Some(&input[1..]))),
        Err(e) => Err(e),
    }
}

pub struct Metric<'a> {
    measurement: &'a[u8],
    tags: Vec<(&'a[u8], &'a[u8])>,
    fields: Vec<(&'a[u8], &'a[u8])>,
    timestamp: Option<&'a[u8]>,
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
        Ok((_remaining, value)) => {
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
        Ok((_remaining, (name, value))) => {
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
        Ok((_remaining, (name, value))) => {
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
        Ok((_remaining, (name, value))) => {
            assert_eq!(name, b"method");
            assert_eq!(value, b"GET");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

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
        Ok((_remaining, timestamp)) => {
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
        Ok((_remaining, timestamp)) => {
            assert_eq!(timestamp.unwrap(), b"1234567890");
        }
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_tag_with_trailing_comma() {
    let t = b",method=GET,blah";
    let r = tag(t);
    match r {
        Ok((_remaining, (key, value))) => {
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
        Ok((_remaining, (key, value))) => {
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
        Ok((_remaining, vec)) => {
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
        Ok((_remaining, vec)) => {
            assert_eq!(vec.len(), 2);
            let (key, value) = vec[0];
            assert_eq!(key, b"count");
            assert_eq!(value, b"1");
            let (key, value) = vec[1];
            assert_eq!(key, b"duration");
            assert_eq!(value, b"5");
        },
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    };
}

#[test]
fn check_metric() {
    let t = b"requests,method=GET duration=101 123456789\n";
    let r = metric(t);
    match r {
        Ok((_remaining, metric)) => {
            assert_eq!(metric.measurement, b"requests");
            assert_eq!(metric.tags.len(), 1);
            let (key, value) = metric.tags[0];
            assert_eq!(key, b"method");
            assert_eq!(value, b"GET");
            assert_eq!(metric.fields.len(), 1);
            let (key, value) = metric.fields[0];
            assert_eq!(key, b"duration");
            assert_eq!(value, b"101");
            assert_eq!(metric.timestamp.unwrap(), b"123456789");
        },
        Err(Err::Incomplete(needed)) => panic!("Incomplete: {:?}", needed),
        Err(Err::Error(e)) => panic!("Error: {:?}", e),
        Err(Err::Failure(e)) => panic!("Failure: {:?}", e),
    }
}

