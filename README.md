# Torrent Bootstrap

This utility is designed to solve a few niche purposes when working with torrents.

- Given two incomplete torrents, each with the same file, merge the contents between torrents to generate the most-complete file for each torrent. 
- Re-organize your collection if your files have been disassociated from a common file structure.
- And obviously, scan your collection before loading a new torrent file into your torrent client to avoid downloading extra bytes.

### How to run

```
Usage: torrent_bootstrap [OPTIONS] --torrents <TORRENTS>... --scan <SCAN>... --export <EXPORT>

Options:
      --torrents <TORRENTS>...  Path that should be used to load a torrent
      --scan <SCAN>...          Paths that should be scanned for matching files
      --export <EXPORT>         Path where the exported file should be updated or stored. 
                                Any matching files under this export path are automatically added to the scan path
      --threads <THREADS>       Number of read threads for hashing [default: 1]
      --resize-export-files     If the export file on disk is smaller than the one in the torrent, 
                                then resize to match the torrent. This helps with accuracy during the scanning process
  -h, --help                    Print help
  -V, --version                 Print version
```