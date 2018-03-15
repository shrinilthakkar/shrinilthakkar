{
    "logging": {
        "log_level": "INFO"
    },
    "connections": {
        "cache": {
            "fc": {
                "host": "fcnew-cache.moengage.com",
                "port": 6379,
                "lazy": false,
                "timeout": 5,
                "db": 0
            },
            "fc_v2": {
                "host": "satya-test-hs.wvesfv.clustercfg.use1.cache.amazonaws.com",
                "port": 6379,
                "lazy": false,
                "timeout": 5,
                "db": 0
            },
            "st_fc": {
                "host": "fcsmart-cache.moengage.com",
                "port": 6379,
                "lazy": true,
                "timeout": 5,
                "db": 0
            },
            "st_dedup": {
                "host": "st-dedup-cache.moengage.com",
                "port": 6379,
                "lazy": false,
                "timeout": 5,
                "db": 0
            },
            "local": {
                "host": "127.0.0.1",
                "port": 6379,
                "lazy": true,
                "timeout": 5,
                "db": 0
            },
            "productfeed": {
                "host": "product-enrichment-cache.moengage.com",
                "port": 6379,
                "lazy": true,
                "timeout": 5,
                "db": 0
            },
            "prod": {
                "host": "cacheprod.moengage.com",
                "port": 6379,
                "db": 0,
                "timeout": 5,
                "lazy": true,
                "replica": [
                    {
                        "name": "prod_1",
                        "host": "cacheprod.moengage.com",
                        "port": 6379,
                        "db": 0,
                        "timeout": 5,
                        "lazy": true
                    }
                ]
            },
            "push_campaign": {
                "host": "inapp-dedup-cache.moengage.com",
                "port": 6379,
                "db": 0,
                "timeout": 15,
                "lazy": true
            },
            "smart_trigger": {
                "host": "smarttrigger-cache.moengage.com",
                "port": 6379,
                "db": 0,
                "timeout": 5,
                "lazy": true
            },
            "bfilter": {
                "host": "inappbloom-cache.moengage.com",
                "port": 6379,
                "db": 1,
                "timeout": 15,
                "lazy": true
            },
            "bfilterlocal": {
                "host": "localhost",
                "port": 6379,
                "timeout": 5,
                "db": 0,
                "lazy": true
            },
            "allUsersCache_v2": {
                "host": "allusers-cache.moengage.com",
                "port": 6379,
                "db": 0,
                "timeout": 5,
                "lazy": true
            }
        }
    }
}
