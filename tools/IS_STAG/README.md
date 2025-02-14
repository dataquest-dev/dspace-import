# check_url.py

This script compares the new and current URLs based on the bitstream name. 
This script was developed and used for the IS/STAG check after the bitstream URLs with special characters in the name were replaced by paths containing the bitstream UUIDs.
Use your username (--user) and password (--password) to dspace-ZCU. Use your path (--input_dir) (default: data) and file (--JSON_name).
Use you keys: new_key (--new_key) and cur_key (--cur_key) (default: new_url and cur_url).

```
python check_url.py --endpoint="https://naos-be.zcu.cz/server/api" ----user="[user] "--password=[password] --JSON_name="input.json"
```
