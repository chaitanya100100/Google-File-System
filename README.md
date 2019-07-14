# Google-File-System
A python implementation of Google File System

## Compile
- Run `bash recompile.sh`

## Dependency
- gRPC

## Run
- In one terminal, run `python chunk_server.py` to start chunk servers.
- In other terminal, run `python master_server.py` to start master server.
- From the third terminal, run `python client.py <command> <file_path> <args>` to interact with file system.

### Commands
- `python client.py create <file_path>`
  - Creates a new file with given absolute file path `<file_path>`
- `python client.py list <prefix>`
  - Lists all files whose absolute path have prefix `<prefix>`
- `python client.py append <file_path> <string>`
  - Appends `<string>` to file `<file_path>`
- `python client.py read <file_path> <offset> <len>`
  - Reads `<len>` characters of file `<file_path>` starting from `<offset>`
- `python client.py delete <file_path>`
  - Deletes file `<file_path>`
- `python client.py undelete <file_path>`
  - Restores the deleted file `<file_path>`

## Miscellaneous Details
- `common.py` contains common metadata including port numbers of master server and chunk servers, chunk size, etc. It also contains some common code.
- Proper logging is provided in each of chunk server, master server and client.
- Most of the errors are handled in the code.
- As per original GFS, files are identified by absolute paths. There is no concept of directories. GFS has a module which keeps checks on file path and directory structure consistencies. It is not implemented here.
- The code is gradually extendable by adding features one by one.

## Demo
After starting chunk servers and master server, run the following series of commands to have a glimpse of GFS.
```
python client.py create /file1
python client.py create /dir1/file2
python client.py list /dir1
python client.py append /file1 abcdefghijklm
python client.py read /file1 2 6
python client.py delete /file1
python client.py list /
python client.py undelete /file1
python client.py list /
```
