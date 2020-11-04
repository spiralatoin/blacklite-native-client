# Blacklite Reader

## Date Processing

```
./blacklite-reader \
  --after="2020-11-03 19:22:09" \
  --before="2020-11-03 19:22:11" \
  --timezone=PST \
  /tmp/blacklite/archive.2020-11-03-07-22.669.db
```

## Still dies with exception

Anything with a "where clause" dies.

```
build/graal/blacklite-reader -c --where="limit > 10000" /tmp/blacklite/archive.db 
```

## Working with Binary Content

Extract data using the `binary` flag and redirect to a file.

```bash
./blacklite-reader --binary /tmp/blacklite/archive.db > zarchive.zst
zstd -d zarchive.zst 
```
