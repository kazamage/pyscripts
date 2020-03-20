import json

import redis


def main():
    conn = redis.Redis(host='localhost', port=6379, db=0)
    pubsub = conn.pubsub()
    pubsub.subscribe(['USDJPY'])
    for data in pubsub.listen():
        if data['type'] == 'message':
            print(json.loads(data['data'].decode('utf-8')))


if __name__ == '__main__':
    main()
