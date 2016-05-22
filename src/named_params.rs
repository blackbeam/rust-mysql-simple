use std::borrow::Cow;
use error::{
    Result as MyResult,
};
use error::Error::DriverError;
use error::DriverError::MixedParams;

enum ParserState {
    TopLevel,
    // (string_delimiter, last_char)
    InStringLiteral(char, char),
    MaybeInNamedParam,
    InNamedParam,
}

use self::ParserState::*;

pub fn parse_named_params<'a>(query: &'a str) -> MyResult<(Option<Vec<String>>, Cow<'a, str>)> {
    let mut state = TopLevel;
    let mut have_positional = false;
    let mut cur_param = 0;
    // Vec<(start_offset, end_offset, name)>
    let mut params = Vec::new();
    for (i, c) in query.chars().enumerate() {
        let mut rematch = false;
        match state {
            TopLevel => {
                match c {
                    ':' => state = MaybeInNamedParam,
                    '\'' => state = InStringLiteral('\'', '\''),
                    '"' => state = InStringLiteral('"', '"'),
                    '?' => have_positional = true,
                    _ => (),
                }
            },
            InStringLiteral(separator, prev_char) => {
                match c {
                    x if x == separator && prev_char != '\\' => state = TopLevel,
                    x => state = InStringLiteral(separator, x),
                }
            },
            MaybeInNamedParam => {
                match c {
                    'a' ... 'z' | '_' => {
                        params.push((i - 1, 0, String::with_capacity(16)));
                        params[cur_param].2.push(c);
                        state = InNamedParam;
                    },
                    _ => rematch = true,
                }
            },
            InNamedParam => {
                match c {
                    'a' ... 'z' | '_' => params[cur_param].2.push(c),
                    _ => {
                        params[cur_param].1 = i;
                        cur_param += 1;
                        rematch = true;
                    }
                }
            },
        }
        if rematch {
            match c {
                ':' => state = MaybeInNamedParam,
                '\'' => state = InStringLiteral('\'', '\''),
                '"' => state = InStringLiteral('"', '"'),
                _ => state = TopLevel,
            }
        }
    }
    match state {
        InNamedParam => params[cur_param].1 = query.len(),
        _ => (),
    }
    if params.len() > 0 {
        if have_positional {
            return Err(DriverError(MixedParams));
        }
        let mut real_query = String::with_capacity(query.len());
        let mut last = 0;
        let mut out_params = Vec::with_capacity(params.len());
        for (start, end, name) in params.into_iter() {
            real_query.push_str(&query[last..start]);
            real_query.push('?');
            last = end;
            out_params.push(name);
        }
        real_query.push_str(&query[last..]);
        Ok((Some(out_params), real_query.into()))
    } else {
        Ok((None, query.into()))
    }
}

#[cfg(test)]
mod test {
    use error::Error::DriverError;
    use error::DriverError::MixedParams;
    use named_params::parse_named_params;

    #[test]
    fn should_parse_named_params() {
        let result = parse_named_params(":a :b").unwrap();
        assert_eq!((Some(vec!["a".to_string(), "b".into()]), "? ?".into()), result);

        let result = parse_named_params("SELECT (:a-10)").unwrap();
        assert_eq!((Some(vec!["a".to_string()]), "SELECT (?-10)".into()), result);

        let result = parse_named_params(r#"SELECT '"\':a' "'\"':c" :b"#).unwrap();
        assert_eq!((Some(vec!["b".to_string()]), r#"SELECT '"\':a' "'\"':c" ?"#.into()), result);

        let result = parse_named_params(r":a_Aa:b").unwrap();
        assert_eq!((Some(vec!["a_".to_string(), "b".into()]), r"?Aa?".into()), result);

        let result = parse_named_params(r"::b").unwrap();
        assert_eq!((Some(vec!["b".to_string()]), r":?".into()), result);

        let err = parse_named_params(r":a ?").unwrap_err();
        match err {
            DriverError(MixedParams) => (),
            _ => unreachable!(),
        }
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use test;
        use named_params::parse_named_params;

        #[bench]
        fn parse_ten_named_params(bencher: &mut test::Bencher) {
            bencher.iter(|| {
                let result = parse_named_params(r#"
                SELECT :one, :two, :three, :four, :five, :six, :seven, :eight, :nine, :ten
                "#).unwrap();
                test::black_box(result);
            });
        }

        #[bench]
        fn parse_zero_named_params(bencher: &mut test::Bencher) {
            bencher.iter(|| {
                let result = parse_named_params(r"
                SELECT one, two, three, four, five, six, seven, eight, nine, ten
                ").unwrap();
                test::black_box(result);
            });
        }
    }
}
