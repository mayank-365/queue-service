package com.example.constants;

import com.example.utils.Utils;

public class Constants {

    public static final String PROP_FILENAME = "config.properties";
    public static final long VISIBILITY_TIMEOUT = Utils.getVisibilityTimeout();

    public static final String REDIS_ZADD_COMMAND = "zadd";
    public static final String REDIS_REV_RANGE_COMMAND = "zrevrange";
    public static final String REDIS_ZREM_COMMAND = "zrem";
    public static final RedisConf REDIS_CONF = Utils.getRedisConfig();
    public static final String REDIS_ZRANGEBYSCORE_COMMAND = "zrangebyscore";


}
