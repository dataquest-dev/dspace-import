# bitstream_metadata.py

This script collects sizeBytes of bitstreams based on their UUIDs.

```
python bitstream_metadata.py  --endpoint https://naos-be.zcu.cz/server/api --user=YYY --password=ZZZ --input-dir=XXX --JSON-name=YYY --cache-create 
```

and then

```
python bitstream_metadata.py --input-dir=XXX --JSON-name=YYY --cache-use
```
