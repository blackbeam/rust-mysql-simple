use nom::{IResult, le_u8, le_u16, le_u24, le_u32, le_u64};
use nom::IResult::*;

#[inline]
pub fn lenenc_int(i: &[u8]) -> IResult<&[u8], (u8, u64)> {
    let (i, len) = try_parse!(i, le_u8);
    match len {
        0xfc => map!(i, le_u16, |x| (3, u64::from(x))),
        0xfd => map!(i, le_u24, |x| (4, u64::from(x))),
        0xfe => map!(i, le_u64, |x| (9, x)),
        x => Done(i, (1, x as u64)),
    }
}

#[inline]
pub fn lenenc_bytes(i: &[u8]) -> IResult<&[u8], (u64, &[u8])> {
    let (i, (cons, len)) = try_parse!(i, lenenc_int);
    map!(i, take!(len), |x| (cons as u64 + len, x))
}

/// Description: https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41
///
/// ```ignore
/// (
///     schema,
///     table,
///     org_table,
///     name,
///     org_name,
///     character_set,
///     column_length,
///     column_type,
///     flags,
///     decimals,
///     default_values,
/// )
/// ```
named!(pub column_def<&[u8], (&[u8], &[u8], &[u8], &[u8], &[u8], u16, u32, u8, u16, u8, Option<&[u8]>)>,
    do_parse! (
        take!(4) >>
        schema: call!(lenenc_bytes) >>
        table: call!(lenenc_bytes) >>
        org_table: call!(lenenc_bytes) >>
        name: call!(lenenc_bytes) >>
        org_name: call!(lenenc_bytes) >>
        take!(1) >>
        character_set: call!(le_u16) >>
        column_length: call!(le_u32) >>
        column_type: call!(le_u8) >>
        flags: call!(le_u16) >>
        decimals: call!(le_u8) >>
        take!(2) >>
        len: opt!(complete!(call!(lenenc_int))) >>
        default_values: cond!(
            len.is_some(),
            take!(len.unwrap().1)
        ) >>
        (
            (
                schema.1,
                table.1,
                org_table.1,
                name.1,
                org_name.1,
                character_set,
                column_length,
                column_type,
                flags,
                decimals,
                default_values,
            )
        )
    )
);

#[cfg(test)]
mod test {
    use nom::IResult::Done;
    use super::*;

    #[test]
    fn should_parse_column_definition() {
        let payload1 = b"\x03def\x06schema\x05table\x09org_table\x04name\x08org_name\
                         \x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let payload2 = b"\x03def\x06schema\x05table\x09org_table\x04name\x08org_name\
                         \x0c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x07default";
        let out1 = column_def(&payload1[..]);
        let out2 = column_def(&payload2[..]);
        println!("{:?}", out1);
        assert!(
            out1 == Done(&[][..], (b"schema", b"table", b"org_table", b"name", b"org_name",
                                   0, 0, 0, 0, 0, None))
        );
        assert!(
            out2 == Done(&[][..], (b"schema", b"table", b"org_table", b"name", b"org_name",
                                   0, 0, 0, 0, 0, Some(b"default")))
        )
    }

    #[cfg(feature = "nightly")]
    mod bench {
        use test;
        use super::super::*;
        use Column;

        static COLUMN_PAYLOAD: &'static [u8] = b"\x03def\x06schema\x05table\x09org_table\x04name\
                                                 \x08org_name\x0c\x00\x00\x00\x00\x00\x0F\x00\x00\
                                                 \x00\x00\x00\x00\x07default";

        #[bench]
        fn nom_column(bencher: &mut test::Bencher) {
            bencher.iter(|| {
                let out = column_def(&COLUMN_PAYLOAD[..]);
                test::black_box(out);
            });
        }

        #[bench]
        fn new_column(bencher: &mut test::Bencher) {
            bencher.iter(|| {
                let pld = unsafe {
                    ::std::vec::Vec::from_raw_parts(COLUMN_PAYLOAD.as_ptr() as *mut u8,
                                                    COLUMN_PAYLOAD.len(),
                                                    COLUMN_PAYLOAD.len())
                };
                let out = Column::from_payload(pld);
                test::black_box(::std::mem::forget(out));
            });
        }
    }
}
