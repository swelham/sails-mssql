![image_squidhome@2x.png](http://i.imgur.com/RIvu9.png) 

# sails-mssql

This is the salils adapter for using Microsoft SQL Server with Sails.js.

# node-sqlserver setup

Instructions on how to install the node-sqlserver module can be found at https://github.com/WindowsAzure/node-sqlserver. 

However this can be a little tricky and problematic, so you can download or clone the built module from https://github.com/swelham/node-sqlserver-build.
Extract the zip file into the node_modules folder of the sails-mssql module and you should be ready to go.

# sails configuration

Open the config/adapter.js file from the root of your sails application and configure sails-mssql like so.

```
module.exports.adapters = {

  'default': 'mssql',

  mssql: {
    module: 'sails-mssql',
    connectionString: 'Driver={SQL Server Native Client 11.0};Server=Your Server;Database=Your Database;Trusted_Connection={Yes}'
  }
};
```

# todo

* ~~Required CRUD methods~~
* Add pagination support
* ~~Optional CRUD methods~~
    * ~~createEach~~
    * ~~findOrCreate~~
    * ~~findOrCreateEach~~
* ~~Database generation~~
* Refactor all queries with a criteria to use sql parameters
* Batch queries to prevent multiple database calls (such as createEach, findOrCreate, etc...)
* ~~Need some basic usage documentation~~


## about sails.js and waterline
http://SailsJs.com

Waterline is a new kind of storage and retrieval engine for Sails.js.  It provides a uniform API for accessing stuff from different kinds of databases, protocols, and 3rd party APIs.  That means you write the same code to get users, whether they live in mySQL, LDAP, MongoDB, or Facebook.
