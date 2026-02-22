# ibm-cloud-db2-meta

stores stuff in db2. it's a key/value thing. i just was learning imb cloud stuff

## what it does

you type commands, it talks to ibm db2, data gets saved. that's it.

## prepare requirements

- `pip install ibm-db`
- a db2 instance somewhere on ibm cloud

## setup

copy the env file and fill in your db2 credentials

it's works only with ssl file, for simplicity

```bash
cp env.template .env
```

put your actual values in there. don't commit it. you know the drill.
you also need the ssl cert somewhere. point `DB2_SSL_CERT` at it.


## run it

```bash
python main.py
```

## commands

```
set  <key> <value>   saves a thing
get  <key>           gets the thing
del  <key>           bye thing
list                 all the things
find <pattern>       find things (use % as wildcard)
help                 you're probably reading this instead
exit                 leave
```

