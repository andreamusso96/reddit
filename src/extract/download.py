#!/usr/bin/env python3
import subprocess, os, urllib.request

def fetch_torrent_from_url(url, out_path):
    """Fetch a torrent from a URL and save it to a file. A torrent is a file that contains the metadata for downloading stuff 
    So you first need to get the file. Then write it on disk at out_path. Finally, you can use that file on disc + aria to download what you want."""
    with urllib.request.urlopen(url) as r, open(out_path, "wb") as f:
        f.write(r.read())
    return out_path

def show_files(torrent_path):
    """Show the files in the torrent."""
    subprocess.run(["aria2c", "--show-files", torrent_path], check=True)

def download_selected_files_from_torrent(torrent_path: str, download_dir: str, select_indices: str):
    """Download the selected files from the torrent."""
    cmd = [
        "aria2c",
        "--bt-remove-unselected-file=true",
        "--max-download-limit=100M",              # per-download unlimited
        "--bt-max-peers=200",      
        "--file-allocation=none",
        "--seed-ratio=0",
        "--seed-time=0",
        "--max-overall-download-limit=0",
        f"--select-file={select_indices}",
        f"--dir={os.path.abspath(download_dir)}",
        torrent_path
    ]
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    config = {
        'torrent_url': 'https://academictorrents.com/download/30dee5f0406da7a353aff6a8caa2d54fd01f2ca1.torrent',
        'torrent_path': '/Users/andrea/Desktop/PhD/Data/Pipeline/reddit/src/extract/reddit_comments_submissions_2005_06_2025_06.torrent',
        'download_dir': '/Users/andrea/Desktop/PhD/Data/Pipeline/reddit/downloads',
        'select_indices': '456-476,215-235'
    }
    download_selected_files_from_torrent(torrent_path=config['torrent_path'], download_dir=config['download_dir'], select_indices=config['select_indices'])

    # Show the torrent files
    # fetch_torrent_from_url(url=config['torrent_url'], out_path=config['torrent_path'])
    # show_files(torrent_path=config['torrent_path'])