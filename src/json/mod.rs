#[cfg(feature = "rustc_serialize")]
pub mod rustc_integration;
#[cfg(not(feature ="rustc_serialize"))]
pub mod serde_integration;



/// Use it to pass `T: Encodable` as JSON to a prepared statement.
///
/// ```ignore
/// #[derive(RustcEncodable)]
/// struct EncodableStruct {
///     // ...
/// }
///
/// conn.prep_exec("INSERT INTO table (json_column) VALUES (?)",
///                (Serialized(EncosdableStruct),));
/// ```
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug, Hash)]
pub struct Serialized<T>(pub T);

/// Use it to parse `T: Decodable` from `Value`.
///
/// ```ignore
/// #[derive(RustcDecodable)]
/// struct DecodableStruct {
///     // ...
/// }
/// // ...
/// let (Deserialized(val),): (Deserialized<DecodableStruct>,)
///     = from_row(row_with_single_json_column);
/// ```
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Debug, Hash)]
pub struct Deserialized<T>(pub T);

#[derive(Debug)]
pub struct DeserializedIr<T> {
    bytes: Vec<u8>,
    output: Deserialized<T>,
}
