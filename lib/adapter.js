/*---------------------------------------------------------------
  :: sails-mssql
  -> adapter
---------------------------------------------------------------*/

var sqlserver = require('node-sqlserver'),
	utils = require('./utils'),
	sql = require('./sql'),
	_ = require('underscore'),
	util = require('util');

module.exports = (function () {
	
	var collections = {};

	var adapter = {

		// Set to true if this adapter supports (or requires) things like data types, validations, keys, etc.
		// If true, the schema for models using this adapter will be automatically synced when the server starts.
		// Not terribly relevant if not using a non-SQL / non-schema-ed data store
		syncable: true,

		// Default configuration for collections
		// (same effect as if these properties were included at the top level of the model definitions)
		defaults: {
		},

		// This method runs when a model is initially registered at server start time
		registerCollection: function (collection, cb) {
			var def = _.clone(collection);
			var key = def.identity;

			if (collections[key]) return cb();

			collections[key] = def;

			cb();
		},


		// The following methods are optional
		////////////////////////////////////////////////////////////

		// Optional hook fired when a model is unregistered, typically at server halt
		// useful for tearing down remaining open connections, etc.
		/*teardown: function (cb) {
			cb();
		},*/


		// REQUIRED method if integrating with a schemaful database
		define: function (collectionName, definition, cb) {
			
			// Define a new "table" or "collection" schema in the data store
			cb();
		},

		// REQUIRED method if integrating with a schemaful database
		describe: function (collectionName, cb) {
			
			// Respond with the schema (attributes) for a collection or table in the data store
			var attributes = {};
			cb(null, attributes);
		},

		// REQUIRED method if integrating with a schemaful database
		drop: function (collectionName, cb) {
			// Drop a "table" or "collection" schema from the data store
			cb();
		},

		// Optional override of built-in alter logic
		// Can be simulated with describe(), define(), and drop(),
		// but will probably be made much more efficient by an override here
		// alter: function (collectionName, attributes, cb) { 
		// Modify the schema of a table or collection in the data store
		// cb(); 
		// },


		create: function (collectionName, values, cb) {
			Object.keys(values).forEach(function (key) {
				values[key] = utils.prepareValue(values[key]);
			});

			var query = sql.insertQuery(collectionName, values);

			execQuery(collectionName, query, function (err, results) {
				if (err) return cb(err);

				if (results.rows && results.rows.length > 0) {
					var id = results.rows[0][0];

					if (id) {
						values = _.extend({}, values, { id: id });
					}
				}

				values = utils.castFromSql(getCollectionSchema(collectionName), values);
				
				cb(null, values);
			});
		},

		find: function (collectionName, options, cb) {
			var query = sql.selectQuery(collectionName, options);
			
			execQuery(collectionName, query, function (err, results) {
				if (err) return cb(err);

				var values = [];

				if (results.rows && results.rows.length > 0) {
					results.rows.forEach(function (row) {
						var value = {};

						results.meta.forEach(function (meta, index) {
							value[meta.name] = row[index];
						});

						value = utils.castFromSql(getCollectionSchema(collectionName), value);

						values.push(value);
					});
				}

				cb(null, values);
			});
		},

		// REQUIRED method if users expect to call Model.update()
		update: function (collectionName, options, values, cb) {
			var query = util.format('SELECT id FROM [%s] %s',
				collectionName,
				sql.serializeOptions(collectionName, options));

			execQuery(collectionName, query, function (err, results) {
				if (err) return cb(err);

				var ids = [];

				if (results.rows && results.rows.length > 0) {
					results.rows.forEach(function (row) {
						ids.push(row[0]);
					});
				}

				Object.keys(values).forEach(function(value) {
					values[value] = utils.prepareValue(values[value]);
				});

				var updateQuery = util.format('UPDATE [%s] SET %s %s',
					collectionName,
					sql.updateCriteria(collectionName, values),
					sql.serializeOptions(collectionName, options));

				execQuery(collectionName, updateQuery, function (err, updateResults) {
					if (err) return cb(err);

					var criteria;

					if(ids.length === 1) {
						criteria = { where: { id: ids[0] }, limit: 1 };
					} else {
						criteria = { where: { id: ids }};
					}

					adapter.find(collectionName, criteria, function (err, users) {
						if (err) return cb(err);

						var updatedValues = [];

						if (users && users.length > 0) {
							users.forEach(function (user) {
								var value = {};

								Object.keys(user).forEach(function (key) {
									value[key] = user[key];
								});

								value = utils.castFromSql(getCollectionSchema(collectionName), value);

								updatedValues.push(value);
							});
						}

						cb(null, updatedValues);
					});
				});
			});
		},

		// REQUIRED method if users expect to call Model.destroy()
		destroy: function (collectionName, options, cb) {
			var query = sql.deleteQuery(collectionName, options);

			execQuery(collectionName, query, function (err, results) {
				if (err) return cb(err);

				cb();
			});
		},

		// REQUIRED method if users expect to call Model.stream()
		stream: function (collectionName, options, stream) {
			// options is a standard criteria/options object (like in find)

			// stream.write() and stream.end() should be called.
			// for an example, check out:
			// https://github.com/balderdashy/sails-dirty/blob/master/DirtyAdapter.js#L247

		}

		/*
		**********************************************
		* Optional overrides
		**********************************************

		// Optional override of built-in batch create logic for increased efficiency
		// otherwise, uses create()
		createEach: function (collectionName, cb) { cb(); },

		// Optional override of built-in findOrCreate logic for increased efficiency
		// otherwise, uses find() and create()
		findOrCreate: function (collectionName, cb) { cb(); },

		// Optional override of built-in batch findOrCreate logic for increased efficiency
		// otherwise, uses findOrCreate()
		findOrCreateEach: function (collectionName, cb) { cb(); }
		*/


		/*
		**********************************************
		* Custom methods
		**********************************************

		////////////////////////////////////////////////////////////////////////////////////////////////////
		//
		// > NOTE:  There are a few gotchas here you should be aware of.
		//
		//    + The collectionName argument is always prepended as the first argument.
		//      This is so you can know which model is requesting the adapter.
		//
		//    + All adapter functions are asynchronous, even the completely custom ones,
		//      and they must always include a callback as the final argument.
		//      The first argument of callbacks is always an error object.
		//      For some core methods, Sails.js will add support for .done()/promise usage.
		//
		//    + 
		//
		////////////////////////////////////////////////////////////////////////////////////////////////////


		// Any other methods you include will be available on your models
		foo: function (collectionName, cb) {
		cb(null,"ok");
		},
		bar: function (collectionName, baz, watson, cb) {
		cb("Failure!");
		}


		// Example success usage:

		Model.foo(function (err, result) {
		if (err) console.error(err);
		else console.log(result);

		// outputs: ok
		})

		// Example error usage:

		Model.bar(235, {test: 'yes'}, function (err, result){
		if (err) console.error(err);
		else console.log(result);

		// outputs: Failure!
		})

		*/
	};

	//////////////                 //////////////////////////////////////////
	////////////// Private Methods //////////////////////////////////////////
	//////////////                 //////////////////////////////////////////

	function getCollectionConfig (collectionName) {
		return collections[collectionName].config;
	}

	function getCollectionSchema (collectionName) {
		return collections[collectionName].definition;
	}

	function execQuery (collectionName, query, cb) {
		var config = getCollectionConfig(collectionName);

		sqlserver.open(config.connectionString, function (err, conn) {
			if (err) return cb(err);

			conn.queryRaw(query, function (err, results) {
				if (err) return cb(err);

				cb(null, results);
			});
		});
	}

	return adapter;
})();

