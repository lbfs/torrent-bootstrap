use std::path::PathBuf;

pub struct PieceMatchResult {
    pub bytes: Vec<u8>,
    pub paths: Vec<PathBuf>,
}
