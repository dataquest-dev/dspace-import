# check_url.py

This script compares the new and current URLs based on the bitstream name. 
It was developed and used for the IS/STAG check after the bitstream URLs with special characters in the name were replaced by paths containing the bitstream UUIDs.

Parameters:
- Authentication: username (--user) and password (--password) for dspace-ZCU
- Input: directory path (--input_dir) (default: data) and file name (--JSON_name)
- URL keys: new URL key (--new_key) and current URL key (--curr_key) (default: new_url and cur_url)

```
python check_url.py --endpoint="https://naos-be.zcu.cz/server/api" ----user="[user] "--password=[password] --JSON_name="input.json"
```
