use crate::myc::packets::Column;
use byteorder::LittleEndian as LE;
use byteorder::ReadBytesExt;
use std::io;

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct InnerStmt {
    /// Positions and names of named parameters
    named_params: Option<Vec<String>>,
    params: Option<Vec<Column>>,
    columns: Option<Vec<Column>>,
    statement_id: u32,
    num_columns: u16,
    num_params: u16,
    warning_count: u16,
}

impl InnerStmt {
    pub fn from_payload(pld: &[u8], named_params: Option<Vec<String>>) -> io::Result<InnerStmt> {
        let mut reader = &pld[1..];
        let statement_id = reader.read_u32::<LE>()?;
        let num_columns = reader.read_u16::<LE>()?;
        let num_params = reader.read_u16::<LE>()?;
        let warning_count = reader.read_u16::<LE>()?;
        Ok(InnerStmt {
            named_params: named_params,
            statement_id: statement_id,
            num_columns: num_columns,
            num_params: num_params,
            warning_count: warning_count,
            params: None,
            columns: None,
        })
    }

    pub fn set_named_params(&mut self, named_params: Option<Vec<String>>) {
        self.named_params = named_params;
    }

    pub fn set_params(&mut self, params: Option<Vec<Column>>) {
        self.params = params;
    }

    pub fn set_columns(&mut self, columns: Option<Vec<Column>>) {
        self.columns = columns
    }

    pub fn columns(&self) -> Option<&[Column]> {
        self.columns.as_ref().map(AsRef::as_ref)
    }

    pub fn params(&self) -> Option<&[Column]> {
        self.params.as_ref().map(AsRef::as_ref)
    }

    pub fn id(&self) -> u32 {
        self.statement_id
    }

    pub fn num_params(&self) -> u16 {
        self.num_params
    }

    pub fn num_columns(&self) -> u16 {
        self.num_columns
    }

    pub fn named_params(&self) -> Option<&Vec<String>> {
        self.named_params.as_ref()
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod test {
    pub use super::super::consts;
    pub use std::iter;
}
