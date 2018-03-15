#CHANGELOG

v1.4.0:
- EU cluster related changes
- Config management
- DAO Index creator

v1.3.1:
- S3 config read from machine instead of config file
- Mongo DAO - Added support for aggregations
- Added common broker config for MoEngage Workers

v1.3.0:
- Added pushall support
- Cache end points changed to route53
- New oplog Consumers

v1.2.1:
- Action Map changes - get by query and get by platform
- Watchdog Sampling implemented
- Added execution context

v1.2.0:
- Beat creation support in moengage worker
- Changed env fetching to use priority as file->ENV_VARIABLE->prod
- Added a method to hash any python object

v1.1.13:
- Added common config provider for attributes
- Reverse key mapping added

v1.1.12:
- Added common config provider for attributes
- Added Redis MemCached
- Made base model pickleable

v1.1.11:
- Added web attrs in default user attributes
- Added SMS as a valid platform
- Increased action blacklisting limit

v1.1.10:
- Added Platform Enum
- Added Platform based fetching for user attributes and actions

v1.1.9:
- Minor fixes to Serializable object and MemCached
- New watchdog metrics fixed for db wise tracking and whitelisting
- Added support to handle invalid fields in SimpleSchemaDocument

v1.1.8:
- Added process monitor base classes
- New watchdog implementation using InfluxDB
- Added sentry info logs

v1.1.7:
- Added copy methods in base dao and model
- Added dictionary get access methods
- Added provision to create indexes via new daos

v1.1.6:
- New action and user attributes refactored
- Added categories and default attribute tracking

v1.1.5:
- Added unknown platform in config
- Added user_feed in metrics whitelist list in config

v1.1.4:
- Fixed unicode support
- Fixed indexes for new action/user attributes

v1.1.3:
- Action attr refactor to new structure
- Oplog consumer refactor
- Linter related fixes as reported by pylint

v1.1.2:
- Fixed an issue with sentry - for services with no default value
- Fixed unicode handling in MemCached

v1.1.1:
- Refactored sentry
- Implemented new user and action attr tracking
- Added watchdog metrics tracking
- Fixed bug in treysor while generating log statements

v1.1.0:
- Added webhook as platform
- Added raw email sender
- Added File and S3 utils
- Added pykafka consumer and unified logs parser
- Added feature to create indexes at runtime via BaseDAO
- Moved oplog base structures from segmentation

v1.0.3:
- Apscheduler logs moved to warning
- Suppressed sentry info logs and alerts

v1.0.2:
- Sentry added to track service state

v1.0.1:
- Reduced memcached GC time
- Changed IPs to Route53 Urls for mongos
- Added support for list of strings in statsd