#coding:utf-8

import random

def create_random_type_keys(client, number):
    """
    在数据库中创建指定数量的键，键的类型随机。
    """
    for i in range(number):

        key = "key:{0}".format(i)

        create_key_func = random.choice([create_string, create_hash, create_list, create_set, create_zset])

        create_key_func(client, key)


def create_string(client, key):
    client.set(key, "")

def create_hash(client, key):
    client.hset(key, "", "")

def create_list(client, key):
    client.rpush(key, "")

def create_set(client, key):
    client.sadd(key, "")

def create_zset(client, key):
    client.zadd(key, 0, "")


if __name__ == "__main__":

    from redis import Redis

    r = Redis()

    # 测试各个类型键的创建函数

    r.flushdb()

    create_string(r, "str_key")
    assert(r.type("str_key") == "string")

    create_hash(r, "hash_key")
    assert(r.type("hash_key") == "hash")

    create_list(r, "list_key")
    assert(r.type("list_key") == "list")

    create_set(r, "set_key")
    assert(r.type("set_key") == "set")

    create_zset(r, "zset_key")
    assert(r.type("zset_key") == "zset")

    # 测试随机创建函数

    r.flushdb()

    create_random_type_keys(r, 10)

    assert(r.dbsize() == 10)

    for k in r.keys("*"):
        print("Keyname:<{0}> - Type:<{1}>".format(k, r.type(k)))
