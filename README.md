# Torrent Bootstrap

This utility is designed to solve a few niche purposes when working with torrents.

- Given two incomplete torrents, each with the same file, merge the contents between torrents to generate the most-complete file for each torrent. 
- Re-organize your collection if your files have been disassociated from a common file structure.
- And obviously, scan your collection before loading a new torrent file into your torrent client to avoid downloading extra bytes.

### How to run

```
Usage: torrent_bootstrap --torrents <TORRENTS>... --scan <SCAN>... --export <EXPORT> --threads <THREADS>

Options:
      --torrents <TORRENTS>...  Path that should be used to load a torrent
      --scan <SCAN>...          Absolute path that should be scanned to find identical pieces
      --export <EXPORT>         Absolute path where the merged or updated file should be placed
      --threads <THREADS>       Number of threads to perform scanning and hashing
  -h, --help                    Print help
  -V, --version                 Print version
```