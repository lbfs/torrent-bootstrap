#[derive(Debug)]

pub enum TorrentErrorKind {
    MalformedData,
}

#[derive(Debug)]
pub struct TorrentError {
    pub kind: TorrentErrorKind,
    pub message: String
}

impl TorrentError {
    pub fn new(kind: TorrentErrorKind, message: String) -> TorrentError {
        TorrentError { kind, message }
    }
}