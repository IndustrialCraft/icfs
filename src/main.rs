use std::collections::{HashMap, HashSet};
use std::env;
use std::ffi::{OsStr, OsString};
use std::time::{Duration, SystemTime};
use fuser::{FileAttr, Filesystem, FileType, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyWrite, Request};
use fuser::MountOption::NoSuid;

struct ICFS{
    files: FileStorage,
    inode_to_file: HashMap<u64, FileStoragePath>,
    file_to_inode: HashMap<FileStoragePath, u64>,
    unused_inodes: HashSet<u64>,
}
impl ICFS{
    pub fn new() -> Self{
        let mut fs = ICFS{
            files: FileStorage::new(),
            inode_to_file: HashMap::new(),
            file_to_inode: HashMap::new(),
            unused_inodes: HashSet::new()
        };
        fs.create_inode(FileStoragePath::root());
        fs
    }
    pub fn create_inode(&mut self, path: FileStoragePath) -> u64{
        if let Some(inode) = self.file_to_inode.get(&path){
            return *inode;
        }
        let inode = if let Some(inode) = self.unused_inodes.iter().next().cloned(){
            self.unused_inodes.remove(&inode);
            inode
        } else {
            self.inode_to_file.len() as u64 + 1
        };
        self.file_to_inode.insert(path.clone(), inode);
        self.inode_to_file.insert(inode, path);
        inode
    }
    pub fn remove_inode(&mut self, inode: u64) {
        let path = if let Some(path) = self.inode_to_file.remove(&inode){
            path
        } else {
            eprintln!("trying to remove non-existent inode");
            return;
        };
        self.file_to_inode.remove(&path);
        self.unused_inodes.insert(inode);
    }
    pub fn get_entry(&self, inode: u64) -> Option<&FileStorageEntry>{
        let path = if let Some(path) = self.inode_to_file.get(&inode){
            path
        } else {
            return None;
        };
        self.files.lookup(path)
    }
    pub fn get_entry_mut(&mut self, inode: u64) -> Option<&mut FileStorageEntry>{
        let path = if let Some(path) = self.inode_to_file.get(&inode){
            path
        } else {
            return None;
        };
        self.files.lookup_mut(path)
    }
    pub fn get_inode_attrs(&self, inode: u64) -> FileAttr{
        let entry = self.get_entry(inode).unwrap();
        let ts = SystemTime::UNIX_EPOCH;
        FileAttr {
            ino: inode,
            size: match entry{
                FileStorageEntry::File(data) => data.len() as u64,
                FileStorageEntry::Directory(_) => 0,
            },
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts,
            kind: match entry{
                FileStorageEntry::File(_) => FileType::RegularFile,
                FileStorageEntry::Directory(_) => FileType::Directory
            },
            perm: 0o777,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
            flags: 0,
        }
    }
}

impl Filesystem for ICFS {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let entry = self.get_entry(parent);
        match entry{
            Some(entry) => {
                match entry{
                    FileStorageEntry::File(_) => {
                        reply.error(libc::ENOTDIR);
                    }
                    FileStorageEntry::Directory(directory) => {
                        if !directory.contains_key(name){
                            reply.error(libc::ENOENT);
                            return;
                        }
                        let inode = self.create_inode(self.inode_to_file.get(&parent).unwrap().with_pushed(name));
                        reply.entry(&Duration::new(1, 0), &self.get_inode_attrs(inode), 0);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT)
            }
        }
    }
    fn forget(&mut self, _req: &Request<'_>, ino: u64, _nlookup: u64) {
        println!("forget inode {ino}");
        self.remove_inode(ino);
    }
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let entry = self.get_entry(ino);
        match entry{
            Some(_) => {
                let ttl = Duration::new(1, 0);
                reply.attr(&ttl, &self.get_inode_attrs(ino));
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }
    fn mkdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, _mode: u32, _umask: u32, reply: ReplyEntry) {
        let entry = self.get_entry_mut(parent);
        match entry{
            Some(entry) => {
                match entry {
                    FileStorageEntry::File(_) => {
                        reply.error(libc::ENOTDIR);
                    }
                    FileStorageEntry::Directory(directory) => {
                        if directory.contains_key(name){
                            reply.error(libc::EEXIST);
                            return;
                        }
                        directory.insert(name.to_os_string(), FileStorageEntry::Directory(HashMap::new()));
                        let inode = self.create_inode(self.inode_to_file.get(&parent).unwrap().with_pushed(name));
                        reply.entry(&Duration::new(1, 0), &self.get_inode_attrs(inode), 0);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let entry = self.get_entry_mut(parent);
        match entry{
            Some(entry) => {
                match entry {
                    FileStorageEntry::File(_) => {
                        reply.error(libc::ENOTDIR);
                    }
                    FileStorageEntry::Directory(directory) => {
                        directory.remove(name);
                        reply.ok();
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }
    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let entry = self.get_entry_mut(parent);
        match entry{
            Some(entry) => {
                match entry {
                    FileStorageEntry::File(_) => {
                        reply.error(libc::ENOTDIR);
                    }
                    FileStorageEntry::Directory(directory) => {
                        directory.remove(name);
                        reply.ok();
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }
    fn read(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyData) {
        let entry = self.get_entry(ino);
        match entry{
            Some(entry) => {
                match entry {
                    FileStorageEntry::File(buffer) => {
                        let offset = offset as usize;
                        let size = size as usize;
                        reply.data(&buffer[offset.min(buffer.len())..(offset+size).min(buffer.len())]);
                    }
                    FileStorageEntry::Directory(_) => {
                        reply.error(libc::EISDIR);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }
    fn write(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, data: &[u8], _write_flags: u32, _flags: i32, _lock_owner: Option<u64>, reply: ReplyWrite) {
        let entry = self.get_entry_mut(ino);
        match entry{
            Some(entry) => {
                match entry {
                    FileStorageEntry::File(buffer) => {
                        for (i, byte) in data.iter().enumerate(){
                            let position = offset as usize + i;
                            if position == buffer.len(){
                                buffer.push(*byte);
                            } else if position < buffer.len(){
                                buffer[position] = *byte;
                            } else {
                                panic!("oob write");
                            }
                        }
                        reply.written(data.len() as u32);
                    }
                    FileStorageEntry::Directory(_) => {
                        reply.error(libc::EISDIR);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }
    fn rename(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, newparent: u64, newname: &OsStr, _flags: u32, reply: ReplyEmpty) {
        let entry_old = self.get_entry_mut(parent);
        let file = match entry_old{
            Some(FileStorageEntry::Directory(entry_old)) => {
                entry_old.remove(name)
            }
            Some(FileStorageEntry::File(_)) => {
                reply.error(libc::ENOTDIR);
                return;
            }
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        match file{
            Some(file) => {
                //todo: rollback file on error
                let entry_new = self.get_entry_mut(newparent);
                match entry_new {
                    Some(FileStorageEntry::Directory(directory)) => {
                        if directory.contains_key(newname){
                            reply.error(libc::EEXIST);
                            return;
                        }
                        directory.insert(newname.to_os_string(), file);
                        reply.ok();
                    }
                    Some(FileStorageEntry::File(_)) => {
                        reply.error(libc::ENOTDIR);
                        return;
                    }
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }
    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        if offset != 0{
            reply.ok();
            return;
        }
        let entry = self.get_entry(ino);
        match entry{
            Some(entry) => {
                match entry {
                    FileStorageEntry::File(_) => {
                        reply.error(libc::ENOTDIR);
                    }
                    FileStorageEntry::Directory(directory) => {
                        let entries = directory.keys().cloned().collect::<Vec<_>>();
                        let path = self.inode_to_file.get(&ino).unwrap().clone();
                        let _ = reply.add(ino, 0, FileType::Directory, &".");
                        let _ = reply.add(self.create_inode(path.with_popped()), 1, FileType::Directory, &"..");
                        for (i, entry) in entries.iter().enumerate(){
                            let child_path = path.with_pushed(entry.as_os_str());
                            let file_type = match self.files.lookup(&child_path).unwrap(){
                                FileStorageEntry::File(_) => FileType::RegularFile,
                                FileStorageEntry::Directory(_) => FileType::Directory
                            };
                            let _ = reply.add(self.create_inode(child_path), 2 + i as i64, file_type, entry);
                        }
                        reply.ok();
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }
    fn create(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, _mode: u32, _umask: u32, _flags: i32, reply: ReplyCreate) {
        let entry = self.get_entry_mut(parent);
        match entry{
            Some(entry) => {
                match entry {
                    FileStorageEntry::File(_) => {
                        reply.error(libc::ENOTDIR);
                    }
                    FileStorageEntry::Directory(directory) => {
                        directory.entry(name.to_os_string()).or_insert(FileStorageEntry::File(Vec::new()));
                        let inode = self.create_inode(self.inode_to_file.get(&parent).unwrap().with_pushed(name));
                        reply.created(&Duration::new(1, 0), &self.get_inode_attrs(inode), 0, 0, 0);
                    }
                }
            }
            None => {
                reply.error(libc::ENOENT);
            }
        }
    }
}

fn main() {
    let mountpoint = match env::args().nth(1) {
        Some(path) => path,
        None => {
            println!("Usage: icfs <MOUNTPOINT>");
            return;
        }
    };
    let mut filesystem = ICFS::new();
    match &mut filesystem.files.root{
        FileStorageEntry::File(_) => {}
        FileStorageEntry::Directory(dir) => {
            dir.insert(OsString::from("aaa.txt"), FileStorageEntry::File("fgshndiudfhbsduifsd\n".as_bytes().to_vec()));
            dir.insert(OsString::from("bbb.txt"), FileStorageEntry::File(Vec::new()));
        }
    }
    fuser::mount2(filesystem, &mountpoint, &[MountOption::AllowOther, MountOption::AutoUnmount, NoSuid]).unwrap();
}

pub struct FileStorage{
    root: FileStorageEntry
}
impl FileStorage{
    pub fn new() -> Self{
        FileStorage{
            root: FileStorageEntry::Directory(HashMap::new())
        }
    }
    pub fn lookup(&self, path: &FileStoragePath) -> Option<&FileStorageEntry>{
        let mut current_entry = &self.root;
        for part in &path.parts{
            current_entry = match current_entry {
                FileStorageEntry::Directory(directory) => match directory.get(part.as_os_str()){
                    Some(entry) => entry,
                    None => return None,
                },
                FileStorageEntry::File(_) => return None,
            }
        }
        Some(current_entry)
    }
    pub fn lookup_mut(&mut self, path: &FileStoragePath) -> Option<&mut FileStorageEntry>{
        let mut current_entry = &mut self.root;
        for part in &path.parts{
            current_entry = match current_entry {
                FileStorageEntry::Directory(directory) => match directory.get_mut(part.as_os_str()){
                    Some(entry) => entry,
                    None => return None,
                },
                FileStorageEntry::File(_) => return None,
            }
        }
        Some(current_entry)
    }
}
#[derive(Debug)]
pub enum FileStorageEntry{
    File(Vec<u8>),
    Directory(HashMap<OsString,FileStorageEntry>)
}
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FileStoragePath{
    parts: Vec<OsString>
}
impl FileStoragePath{
    pub fn root() -> Self{
        FileStoragePath{
            parts: Vec::new()
        }
    }
    pub fn with_pushed(&self, next: &OsStr) -> Self{
        let mut parts = self.parts.clone();
        parts.push(next.to_os_string());
        FileStoragePath{
            parts
        }
    }
    pub fn with_popped(&self) -> Self{
        let mut parts = self.parts.clone();
        parts.pop();
        FileStoragePath{
            parts
        }
    }
}