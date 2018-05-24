#!/usr/bin/env python3
# Script to download specified folders from the RADAR FTP with the option to
# set start and end modification dates between which to get files. Needs to be
# rewritten as a class to be less spaghetti.

import ftplib as FTP
import datetime
import os
import re

def parse_folders(ftp, folder, start_date, end_date, root_path, overwrite, exclude_pattern=None):
    try:
        ftp.cwd(folder)
    except FTP.error_perm:
        print('Can\'t change to folder: ', folder)
        print(ftp.pwd())
        return
    dir_contents = ftp.mlsd()
    child_folders = []
    files = []
    for f in dir_contents:
        if f[1]['type'] == 'dir':
            child_folders.append(f[0])
        elif f[1]['type'] == 'file':
            file_time = datetime.datetime.strptime(f[1]['modify'], '%Y%m%d%H%M%S')
            if start_date < file_time and file_time < end_date:
                files.append(f[0])
    if files:
        retrieve_files(ftp, files, start_date, end_date, root_path, overwrite, exclude_pattern)

    for cfolder in child_folders:
        parse_folders(ftp, cfolder, start_date, end_date, root_path, overwrite, exclude_pattern)
    ftp.cwd('..')


def retrieve_files(ftp, files, start_date, end_date, root_path, overwrite, exclude_pattern=None):
    def retrieve_file(ftp, filename, local_dir):
        local_file = os.path.join(local_dir, filename)
        if os.path.isfile(local_file) and not overwrite:
            return
        else:
            with open(local_file, 'wb') as f:
                try:
                    ftp.retrbinary('RETR ' + filename, f.write)
                except FTP.error_perm:
                    print('User does not have permission to read file:',
                          ftp.pwd() + '/' + filename)
                    os.unlink(f.name)

    local_dir = os.path.join(args.outpath, os.path.relpath(ftp.pwd(), args.path))
    os.makedirs(local_dir, exist_ok=True)
    for f in files:
        if exclude_pattern:
            if len(exclude_pattern.findall(f)) > 0:
                pass
        retrieve_file(ftp, f, local_dir)

def ftp_download(folders,
                 ip='159.92.120.21',
                 user='user',
                 password='pass',
                 start_date='1970-01-01',
                 end_date='2200-01-01',
                 date_format='%Y-%m-%d',
                 path='/RADAR-CNS/HDFS_CSV/output/',
                 overwrite=False,
                 exclude_pattern=None):
    ftp = FTP.FTP(ip)
    try:
        ftp.login(user, password)
    except FTP.error_perm:
        print('Incorrect login')
        raise

    try:
        ftp.cwd(path)
    except FTP.error_perm:
        print('Can not change directory to given path: %s', path)
        raise
    start_datetime = datetime.datetime.strptime(start_date, date_format)
    end_datetime = datetime.datetime.strptime(end_date, date_format)
    for folder in folders:
        parse_folders(ftp, folder, start_datetime, end_datetime, path, overwrite, exclude_pattern)
    ftp.close()

# Main
if __name__ == "__main__":
    import argparse
    import getpass
    parser = argparse.ArgumentParser(description='Download specified folders from the RADAR FTP.' \
                                     'Defaults to looking at folders in "/RADAR-CNS/HDFS_CSV/output/"')
    parser.add_argument('-u', '--user', help='The FTP user account',
                        default='username')
    parser.add_argument('-ip', '--hostname', help='The FTP hostname',
                        default='159.92.120.21')
    foldergroup = parser.add_mutually_exclusive_group(required=True)
    foldergroup.add_argument('-f', '--folder',
                             help='A string of the folder to be downloaded',
                             default='')
    foldergroup.add_argument('-fs', '--folders',
                             help='A file containing a list of folders to be downloaded',
                             type=argparse.FileType('r'), default='-')
    parser.add_argument('-p', '--path',
                        help='The path within which to search. Defaults to ' \
                            '"/RADAR-CNS/HDFS_CSV/output/"',
                        default='/RADAR-CNS/HDFS_CSV/output/')
    parser.add_argument('-df', '--dateformat', help='The format of the date supplied in' \
                            'startdate. For use in datetime.strptime(). e.g. "%Y-%m-%d"',
                        default='%Y-%m-%d')
    parser.add_argument('-ds', '--startdate', help='Only downloads files past this date',
                        default='2000-01-01')
    parser.add_argument('-de', '--enddate', help='Only download files up to this date',
                        default='2200-01-01')

    parser.add_argument('--overwrite', help='Flag to overwrite existing files',
                        action='store_true')
    parser.add_argument('-o', '--outpath', help='Parent path to save files to',
                        default='.')
    parser.add_argument('-e', '--exclude', help='Regex pattern to exclude', default=None)

args = parser.parse_args()
if args.folder:
    folders = [args.folder]
else:
    folders = args.folders.read().split()
password = getpass.getpass()
if args.exclude:
    args.exclude = re.compile(args.exclude)

ftp_download(folders=folders, ip=args.hostname, user=args.user,
             password=password, start_date=args.startdate,
             end_date=args.enddate, date_format=args.dateformat,
             path=args.path, overwrite=args.overwrite, exclude_pattern=args.exclude)

