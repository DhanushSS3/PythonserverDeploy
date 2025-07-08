import redis

r = redis.Redis(host='127.0.0.1', port=6379, db=0)
keys = r.keys('last_price:*')
print("Found keys:", keys)