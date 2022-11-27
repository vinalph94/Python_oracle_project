# LIBRARY MANAGEMENT SYSTEM

Clone this repository
```bash
$ git clone https://github.com/PSSABISHEK/python_oracle_project.git
$ cd python_oracle_project
```

Install packages
```bash
python -m pip install cx_Oracle --upgrade
```
## For Windows Download [Oracle Client](https://www.oracle.com/database/technologies/instant-client/microsoft-windows-32-downloads.html)
Extract the zip folder to a location and copy the folder path.

## Create config.py file
Paste and edit this content in the file
```bash
connection_data = {
    'local_path': 'paste_oracle_client_path_here',
    'server_name': 'az6F72ldbp1.az.uta.edu',
    'user_name': 'add_user_name_here',
    'password': 'add_password_here'
}
```

## Run the program

```bash
$ python index.py
```

## Resource Links
1. [Quick Start: Developing Python Applications for Oracle Database](https://www.oracle.com/database/technologies/appdev/python/quickstartpythononprem.html)