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
1. **Python 3.8+** (tested with 3.8.10 and 3.11)

2. Install CLARIN-DSpace7.*. (postgres, solr, dspace backend)

3.1. Clone python-api: https://github.com/dataquest-dev/dspace-python-api (branch `main`) and https://github.com/dataquest-dev/DSpace (branch `dtq-dev`)
3.2. Clone submodules:
3.2.1.: `git submodule update --init libs/dspace-rest-python/`

4. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   pip install -r libs/dspace-rest-python/requirements.txt
   ```
   
   **PostgreSQL adapter note:** If `psycopg2` installation fails (missing PostgreSQL dev headers or C compiler), you can replace `psycopg2` with `psycopg2-binary` in `requirements.txt` for easier installation.

5. Get database dump (old CLARIN-DSpace) and unzip it into `input/dump` directory in `dspace-python-api` project.

***
6. Go to the `dspace/bin` in dspace7 installation and run the command `dspace database migrate force` (force because of local types).
**NOTE:** `dspace database migrate force` creates default database data that may be not in database dump, so after migration, some tables may have more data than the database dump. Data from database dump that already exists in database is not migrated.

7. Create an admin by running the command `dspace create-administrator` in the `dspace/bin`

***
8. Prepare `dspace-python-api` project for migration

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

**Note:** `input/icon/` contains license icons (PNG files).
9. Create CLARIN-DSpace5.* databases (dspace, utilities) from dump.
Run `scripts/start.local.dspace.db.bat` or use `scipts/init.dspacedb5.sh` directly with your database.

***
10. Update `project_settings.py`

***
11. Make sure that handle prefixes are configured in the backend configuration (`dspace.cfg`):
   - Set your main handle prefix in `handle.prefix`
   - Add all other handle prefixes to `handle.additional.prefixes`
   - **Note:** The main prefix should NOT be included in `handle.additional.prefixes`
   - **Example:** 
     ```
     handle.prefix = 123456789
     handle.additional.prefixes = 11858, 11234, 11372, 11346, 20.500.12801, 20.500.12800
     ```

12. Copy `assetstore` from dspace5 to dspace7 (for bitstream import). `assetstore` is in the folder where you have installed DSpace `dspace/assetstore`.

***
13. Import
- **NOTE:** database must be up to date (`dspace database migrate force` must be called in the `dspace/bin`)
- **NOTE:** dspace server must be running
- run command `cd ./src && python repo_import.py`

## !!!Migration notes:!!!
- The values of table attributes that describe the last modification time of dspace object (for example attribute `last_modified` in table `Item`) have a value that represents the time when that object was migrated and not the value from migrated database dump.
- If you don't have valid and complete data, not all data will be imported.
- check if license link contains XXX. This is of course unsuitable for production run!

## Check import consistency

Use `tools/repo_diff` utility, see [README](tools/repo_diff/README.md).

## Testing with Empty Tables

The migration script supports testing functionality with empty tables to verify the import process without actual data. 

### Setup

Before using the `--test` option, you need to create the test JSON file:

1. **Create the test JSON file**: Create a file named `test.json` in the `input/test/` directory with the following content:
   ```json
   null
   ```

2. **Configure the test settings**: The test configuration is set in `src/project_settings.py`:
   ```python
   "input": {
       "test": os.path.join(_this_dir, "../input/test"),
       "test_json_filename": "test.json",
   }
   ```
   
   You can change the `test_json_filename` to use a different filename if needed.

### Usage

To run the migration with empty table testing, use the `--test` option followed by the table names you want to test with empty data.

### Examples

```bash
cd ./src && python repo_import.py --test usermetadatas
```

```bash
cd ./src && python repo_import.py --test usermetadatas resourcepolicies
```

### How it Works

When the `--test` option is specified with table names:
1. Instead of loading actual data from database exports, the system loads the configured test JSON file (default: `test.json`) which contains `null`
2. This simulates empty tables during the import process
3. The migration logic is tested without requiring actual data
4. The test JSON filename can be customized in `project_settings.py` under `"input"["test_json_filename"]`