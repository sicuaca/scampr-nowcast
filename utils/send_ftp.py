from ftplib import FTP, error_perm
import os
import yaml


def send_ftp(host, port, user, passwd, local_file, remotepath, remote_file, delete_remote=False):
    """
    Send one or multiple files to an FTP server.

    Args:
        host (str): FTP host.
        port (int): FTP port.
        user (str): FTP username.
        passwd (str): FTP password.
        local_file (str | list): Local file path or list of paths.
        remotepath (str): Remote directory path.
        remote_file (str | list): Remote file name(s) or list of file names.
        delete_remote (bool): Whether to clear remote directory before upload.
    """
    print(f"Connecting to FTP server {host}:{port} ...")
    with FTP() as ftp:
        try:
            ftp.connect(host, int(port))
            print(f"Logging in as '{user}'")
            ftp.login(user=user, passwd=passwd)

            # Pastikan direktori tujuan ada
            ensure_remote_dirs(ftp, remotepath)
            ftp.cwd(remotepath)

            # Hapus isi direktori jika diminta
            if delete_remote:
                print(f"Clearing remote directory: {remotepath}")
                clear_remote_dir(ftp, remotepath)

            # Normalisasi input menjadi list
            if isinstance(local_file, str):
                local_file = [local_file]
            if isinstance(remote_file, str):
                remote_file = [remote_file] if remote_file else [os.path.basename(f) for f in local_file]

            if len(local_file) != len(remote_file):
                raise ValueError("local_file and remote_file must have the same length when both are lists.")

            # Upload loop
            for lfile, rfile in zip(local_file, remote_file):
                remote_target = os.path.join(remotepath, os.path.basename(rfile))
                print(f"Uploading '{lfile}' → '{remote_target}' ...")
                with open(lfile, 'rb') as file:
                    ftp.storbinary(f"STOR {remote_target}", file)
                print(f"✔ Uploaded '{lfile}' to '{remote_target}'")

        except error_perm as e:
            print(f"FTP permission error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
        else:
            print("All files uploaded successfully.")


def clear_remote_dir(ftp, path):
    """Delete all files in the specified remote directory."""
    try:
        ftp.cwd(path)
        files = ftp.nlst()
        for f in files:
            try:
                ftp.delete(f)
                print(f"Deleted remote file: {f}")
            except error_perm:
                try:
                    clear_remote_dir(ftp, f"{path}/{f}")
                    ftp.rmd(f"{path}/{f}")
                except Exception as e:
                    print(f"Could not remove directory '{f}': {e}")
        ftp.cwd("..")
    except Exception as e:
        print(f"Error clearing directory '{path}': {e}")


def ensure_remote_dirs(ftp, path):
    """Ensure all directories in the given path exist on the remote server."""
    parts = path.strip('/').split('/')
    current = ""
    for part in parts:
        current += f"/{part}"
        try:
            ftp.mkd(current)
            print(f"Created remote directory: {current}")
        except error_perm:
            pass  # already exists
