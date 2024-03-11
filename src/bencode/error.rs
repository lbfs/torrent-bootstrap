#[derive(Debug)]
pub enum BencodeErrorKind {
    MalformedData,
}

#[derive(Debug)]
pub struct BencodeError {
    pub kind: BencodeErrorKind,
    pub message: String
}

impl BencodeError {
    pub fn new(kind: BencodeErrorKind, message: String) -> BencodeError {
        BencodeError { kind, message }
    }
}