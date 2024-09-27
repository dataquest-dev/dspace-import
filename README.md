[![Test dspace on dev-5](https://github.com/dataquest-dev/dspace-blackbox-testing/actions/workflows/test.yml/badge.svg)](https://github.com/dataquest-dev/dspace-blackbox-testing/actions/workflows/test.yml)

# Dspace-python-api
used for blackbox testing, data-ingestion procedures

# How to migrate CLARIN-DSpace5.* to CLARIN-DSpace7.*

### Important:
Make sure that your email server is NOT running because some of the endpoints that are used
are sending emails to the input email addresses. 
For example, when using the endpoint for creating new registration data, 
there exists automatic function that sends email, what we don't want
because we use this endpoint for importing existing data.

### Prerequisites:
1. Install CLARIN-DSpace7.*. (postgres, solr, dspace backend)

2.1. Clone python-api: https://github.com/dataquest-dev/dspace-python-api (branch `main`) and https://github.com/dataquest-dev/DSpace (branch `dtq-dev`)
2.2. Clone submodules:
2.2.1.: `git submodule update --init libs/dspace-rest-python/`

2. Get database dump (old CLARIN-DSpace) and unzip it into `input/dump` directory in `dspace-python-api` project.

***
3. Go to the `dspace/bin` in dspace7 installation and run the command `dspace database migrate force` (force because of local types).
**NOTE:** `dspace database migrate force` creates default database data that may be not in database dump, so after migration, some tables may have more data than the database dump. Data from database dump that already exists in database is not migrated.

4. Create an admin by running the command `dspace create-administrator` in the `dspace/bin`

***
5. Prepare `dspace-python-api` project for migration

- copy the files used during migration into `input/` directory:
```
> ls -R ./input
input:
dump  icon

input/dump:
clarin-dspace.sql  clarin-utilities.sql

input/icon:
aca.png  by.png  gplv2.png  mit.png    ...
```
6. 
7. Create CLARIN-DSpace5.* databases (dspace, utilities) from dump.
Run `scripts/start.local.dspace.db.bat` or use `scipts/init.dspacedb5.sh` directly with your database.

***
9. update `project_settings.py`

***
10. Make sure, your backend configuration (`dspace.cfg`) includes all handle prefixes from generated handle json in property `handle.additional.prefixes`, 
e.g.,`handle.additional.prefixes = 11858, 11234, 11372, 11346, 20.500.12801, 20.500.12800`

11. Copy `assetstore` from dspace5 to dspace7 (for bitstream import). `assetstore` is in the folder where you have installed DSpace `dspace/assetstore`.

***
11. Import
- **NOTE:** database must be up to date (`dspace database migrate force` must be called in the `dspace/bin`)
- **NOTE:** dspace server must be running
- run command `cd ./src && python repo_import.py`

## !!!Migration notes:!!!
- The values of table attributes that describe the last modification time of dspace object (for example attribute `last_modified` in table `Item`) have a value that represents the time when that object was migrated and not the value from migrated database dump.
- If you don't have valid and complete data, not all data will be imported.
- check if license link contains XXX. This is of course unsuitable for production run!

## Check import consistency

Use `tools/repo_diff` utility, see [README](tools/repo_diff/README.md).
