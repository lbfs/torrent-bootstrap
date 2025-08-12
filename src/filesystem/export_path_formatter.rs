use std::path::{Path, PathBuf};

use crate::torrent::{info::get_sha1_hexdigest, Torrent, TorrentFile};

pub trait ExportPathFormatter {
    fn format_multiple_files(torrent_file: &TorrentFile, torrent: &Torrent, export_root: &Path) -> PathBuf;
    fn format_single_file(torrent: &Torrent, export_root: &Path) -> PathBuf;
}

pub struct DefaultExportPathFormatter {}
impl ExportPathFormatter for DefaultExportPathFormatter {
    fn format_multiple_files(torrent_file: &TorrentFile, torrent: &Torrent, export_root: &Path) -> PathBuf {
        let data = Path::new("Data");
        let info_hash_as_human = get_sha1_hexdigest(&torrent.info_hash);
        let info_hash_path = Path::new(&info_hash_as_human);
        let torrent_name = Path::new(&torrent.info.name);

        [export_root, info_hash_path, data, torrent_name, &torrent_file.path.iter().collect::<PathBuf>()]
            .iter()
            .collect()
    }

    fn format_single_file(torrent: &Torrent, export_root: &Path) -> PathBuf {
        let data = Path::new("Data");
        let info_hash_as_human = get_sha1_hexdigest(&torrent.info_hash);
        let info_hash_path = Path::new(&info_hash_as_human);
        let torrent_name = Path::new(&torrent.info.name);

        [export_root, info_hash_path, data, torrent_name]
            .iter()
            .collect()
    }
}

