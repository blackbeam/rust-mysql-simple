use uuid::Uuid;

use super::value::{
    Value,
    FromValue,
    ConvIr,
};
use super::error::{
    Error,
    Result as MyResult,
};

impl Into<Value> for Uuid {
    fn into(self) -> Value {
        Value::Bytes(self.as_bytes().to_vec())
    }
}

#[derive(Debug)]
pub struct UuidIr {
    val: Uuid,
    bytes: Vec<u8>,
}

impl ConvIr<Uuid> for UuidIr {
    fn new(v: Value) -> MyResult<UuidIr> {
        match v {
            Value::Bytes(bytes) => match Uuid::from_bytes(bytes.as_slice()) {
                Ok(val) => Ok(UuidIr { val: val, bytes: bytes }),
                Err(_) => Err(Error::FromValueError(Value::Bytes(bytes))),
            },
            v => Err(Error::FromValueError(v)),
        }
    }
    fn commit(self) -> Uuid {
        self.val
    }
    fn rollback(self) -> Value {
        Value::Bytes(self.bytes)
    }
}

impl FromValue for Uuid {
    type Intermediate = UuidIr;
}

#[cfg(test)]
mod test {
    use super::super::value::Value::Bytes;
    use super::super::from_value;
    use uuid::Uuid;

    #[test]
    fn should_convert_bytes_to_uuid() {
        let bytes = vec![0x0e, 0x87, 0x6d, 0x72, 0xc7, 0xb2, 0x4c, 0x00, 0x8c, 0x56, 0x44, 0xee, 0xac, 0x10, 0x15, 0xd1];
        assert_eq!(
            Uuid::parse_str("0e876d72-c7b2-4c00-8c56-44eeac1015d1").unwrap(),
            from_value::<Uuid>(Bytes(bytes))
        );
    }

    #[test]
    fn should_convert_uuid_to_bytes() {
        let bytes = vec![0x0e, 0x87, 0x6d, 0x72, 0xc7, 0xb2, 0x4c, 0x00, 0x8c, 0x56, 0x44, 0xee, 0xac, 0x10, 0x15, 0xd1];
        assert_eq!(
            Bytes(bytes),
            Uuid::parse_str("0e876d72-c7b2-4c00-8c56-44eeac1015d1").unwrap().into()
        );
    }
}
