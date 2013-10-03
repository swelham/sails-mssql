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

		syncable: true,

		defaults: {
		},

		registerCollection: function (collection, cb) {
			var def = _.clone(collection);
			var key = def.identity;

			if (collections[key]) return cb();

			collections[key] = def;

			cb();
		},

		define: function (collectionName, definition, cb) {
			var def = collections[collectionName];
			var schema = sql.schema(def.identity, def.definition);

			var query = util.format('CREATE TABLE [%s](%s)', def.identity, schema);

			execQuery(collectionName, query, function (err, results) {
				if (err) return cb(err);

				cb();
			});
		},

		describe: function (collectionName, cb) {
			var tableName = collections[collectionName].identity;

			execQuery(tableName, sql.describeTableQuery(collectionName), function (err, tableResults) {
				if (err) return cb(err);

				if (!tableResults.rows || tableResults.rows.length === 0) {
					return cb();
				}

				execQuery(tableName, sql.describeIndexQuery(collectionName), function (err, indexResults) {
					if (err) return cb(err);

					var schema = [];

					tableResults.rows.forEach(function (tableRow) {
						var attribute = {
							Field: tableRow[0],
							Type: tableRow[1],
							Default: tableRow[2] || null
						};

						if (indexResults.rows && indexResults.rows.length > 0) {
							indexResults.rows.forEach(function (indexRow) {
								var key = indexRow[0],
									index = indexRow[1];

								if (key !== attribute.Field) return;

								if (index === 'PRIMARY KEY') {
									attribute.primarykey = true;
									//TODO: set autoIncrement
								}
								else if (index === 'UNIQUE') {
									attribute.unique = true;
								}

								attribute.indexed = true;
							});
						}

						schema.push(attribute);
					});

					var normalizedSchema = sql.normalizeSchema(schema);

					collections[collectionName].schema = normalizedSchema;
					cb(null, normalizedSchema);
				});
			});
		},

		addAttribute: function (collectionName, attrName, attrDef, cb) {
			var query = sql.addColumn(collections[collectionName].identity, attrName, attrDef);

			execQuery(collectionName, query, function (err, results) {
				if (err) return cb(err);

				cb();
			});
		},

		removeAttribute: function (collectionName, attrName, cb) {
			var query = sql.removeColumn(collections[collectionName].identity, attrName);

			execQuery(collectionName, query, function (err, results) {
				if (err) return cb(err);

				cb();
			});
		},

		drop: function (collectionName, cb) {
			var query = util.format('DROP TABLE [%s]', collections[collectionName].identity);

			execQuery(collectionName, query, function (err, results) {
				if (err) return cb(err);

				cb();
			});
		},

		create: function (collectionName, values, cb) {
			Object.keys(values).forEach(function (key) {
				values[key] = utils.prepareValue(values[key]);
			});

			var query = sql.insertQuery(collections[collectionName].identity, values);

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
			var query = sql.selectQuery(collections[collectionName].identity, options);
			
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

		update: function (collectionName, options, values, cb) {
			var query = util.format('SELECT id FROM [%s] %s',
				collections[collectionName].identity,
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
					collections[collectionName].identity,
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

		destroy: function (collectionName, options, cb) {
			var query = sql.deleteQuery(collections[collectionName].identity, options);

			execQuery(collectionName, query, function (err, results) {
				if (err) return cb(err);

				cb();
			});
		},

		/*stream: function (collectionName, options, stream) {
			// options is a standard criteria/options object (like in find)

			// stream.write() and stream.end() should be called.
			// for an example, check out:
			// https://github.com/balderdashy/sails-dirty/blob/master/DirtyAdapter.js#L247
			
		}*/

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
