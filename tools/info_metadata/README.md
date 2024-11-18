# metadata.py

This script collects stats about metadata fields.
```
python metadata.py --endpoint="http://dev-5.pc:84/server/api"
python metadata.py --endpoint="https://XXX/server/api" --fetch-bitstreams --user=YYY --password=ZZZ
```

If multiple runs are expected and the metadata can be cached, use 
```
python metadata.py --endpoint="https://XXX/server/api" --fetch-bitstreams --user=YYY --password=ZZZ --cache-create
```

and then

```
python metadata.py --cache-use
```
