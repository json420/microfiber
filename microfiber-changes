#!/usr/bin/python3

import sys
import json

from microfiber import Database, dc3_env


if len(sys.argv) != 2:
    print('takes exactly one argument DBNAME')
    sys.exit(1)
name = sys.argv[1]
db = Database(name, dc3_env())

last_seq = db.get()['update_seq']
print('[monitoring changes from update_seq {}]'.format(last_seq))
print('')

while True:
    r = db.get('_changes',
        include_docs=True,
        feed='longpoll',
        since=last_seq,
    )
    if last_seq != r['last_seq']:
        last_seq = r['last_seq']
        print('update_seq {}:'.format(last_seq))
        print('-' * 80)
        for row in r['results']:
            print(json.dumps(row['doc'], sort_keys=True, indent=4))
        print('')

