![image_squidhome@2x.png](http://i.imgur.com/RIvu9.png)

# Oracle Database Sails/Waterline Adapter

A [Waterline](https://github.com/balderdashy/waterline) adapter for Oracle Database. May be used in a [Sails](https://github.com/balderdashy/sails) app or anything using Waterline for the ORM.

## Prerequired

This package require installing the oracle instant client and instant client devel downloadable at url :

http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html

for rpm linux :

```bash
$ su -
# yum localinstall oracle-instantclient12.1-basic-12.1.0.2.0-1.x86_64.rpm
# yum localinstall oracle-instantclient12.1-devel-12.1.0.2.0-1.x86_64.rpm
# exit
$ vi ~/.bash_profile
```

add the following lines :

```bash
export OCI_LIB_DIR=/usr/lib/oracle/12.1/client64/lib/
export OCI_INCLUDE_DIR=/usr/include/oracle/12.1/client64/
export OCI_VERSION=12
export ORACLE_HOME=/usr/lib/oracle/12.1/client64
export PATH=$PATH:$ORACLE_HOME/bin
export LD_LIBRARY_PATH=$ORACLE_HOME/lib
```

save and exit,then :

```bash
source ~/.bash_profile
```

## Install

Install is through NPM.

```bash
$ npm install sails-oracledb
```

## Configuration

The following config options are available along with their default values:

```javascript
config: {
    tns: '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = localhost)(PORT = 1521))(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = sails_oracle)))',
    user: 'USER',
    password: ''
};
```

## Run tests

set environment variables to override the default database config for the tests, e.g.:

```bash
$ export WATERLINE_ADAPTER_TESTS_TNS='(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = localhost)(PORT = 1521))(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = sails-oracle)))'
$ export WATERLINE_ADAPTER_TESTS_USER='myuser'
$ export WATERLINE_ADAPTER_TESTS_PASSWORD='mypassword' 
$ npm test
```

## About Waterline

Waterline is a new kind of storage and retrieval engine.  It provides a uniform API for accessing stuff from different kinds of databases, protocols, and 3rd party APIs.  That means you write the same code to get users, whether they live in mySQL, LDAP, MongoDB, or Facebook.

To learn more visit the project on GitHub at [Waterline](https://github.com/balderdashy/waterline).
