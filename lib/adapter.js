/*---------------------------------------------------------------
  :: sails-mssql
  -> adapter
---------------------------------------------------------------*/

var sqlserver = require('node-sqlserver'),
	utils = require('./utils'),
	sql = require('./sql'),
	_ = require('underscore'),
	util = require('util'),
	async = require('async');

module.exports = (function () {
	
	var collections = {};

	var adapter = {

		syncable: true,

		defaults: {
			driver: 'SQL Server Native Client 11.0'
		},

		registerCollection: function (collection, cb) {
			var def = _.clone(collection);
			var key = def.identity;

			if (collections[key]) return cb();

			def.config = marshalConfig(def.config);

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

		query: function (collectionName, query, data, cb) {
			execQuery(collectionName, query, data, cb);
		},

		create: function (collectionName, values, cb) {
			Object.keys(values).forEach(function (key) {
				values[key] = utils.prepareValue(values[key]);
			});

			var query = sql.insertQuery(collections[collectionName].identity, values);

			execQuery(collectionName, query, function (err, results) {
				if (err) return cb(err);

				if (results.rows && results.rows.length > 0) {
					var id;

					for (var i = 0; i < results.meta.length; i++) {
						var meta = results.meta[i];
						
						if (meta.name === 'id') {
							id = results.rows[0][i];
							break;
						}
					}

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

				handleQueryResults(collectionName, results, cb);
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

				if (values.id) delete values.id;
				
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

		createEach: function (collectionName, valuesList, cb) {
			var records = [];

			async.each(valuesList, function (data, cb) {
				adapter.create(collectionName, data, function (err, values) {
					if (err) return cb(err);

					records.push(values);

					cb();
				});
			}, function (err) {
				if (err) return cb(err);

				cb(null, records);
			});
		},

		findOrCreate: function (collectionName, options, values, cb) {
			adapter.find(collectionName, options, function (err, data) {
				if (err) return cb(err);

				if (data && data.length > 0) {
					if (data.length === 1)
						return cb(null, data[0]);
					else
						return cb(new Error(util.format('Multiple \'%s\' records were returned by the findOrCreate', collectionName)));
				}

				adapter.create(collectionName, values, function (err, insertedData) {
					if (err) return cb(err);

					cb(null, insertedData);
				});
			});
		},

		findOrCreateEach: function (collectionName, options, valuesList, cb) {
			var optionsAndValues = [];

			valuesList.forEach(function (item, index) {
				optionsAndValues.push({
					options: options[index],
					values: item
				});
			});
			
			var records = [];

			async.each(optionsAndValues, function (data, cb) {
				adapter.findOrCreate(collectionName, data.options, data.values, function (err, values) {
					if (err) return cb(err);

					records.push(values);

					cb();
				});
			}, function (err) {
				if (err) return cb(err);

				cb(null, records);
			});
		}
	};

	//////////////                 //////////////////////////////////////////
	////////////// Private Methods //////////////////////////////////////////
	//////////////                 //////////////////////////////////////////

	function handleQueryResults (collectionName, results, cb) {
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
	}

	function getCollectionConfig (collectionName) {
		return collections[collectionName].config;
	}

	function getCollectionSchema (collectionName) {
		return collections[collectionName].definition;
	}

	function marshalConfig (config) {
		if (config.connectionString) return config;
		if (!config.connection) throw new Error('Sails-MSSQL: No connection configuration provided');

		var mergedConfig = _.extend({}, adapter.defaults, config.connection);
 
		var connectionString = '';

		Object.keys(mergedConfig).forEach(function(key) {
			var value = mergedConfig[key];

			if (value.indexOf(';') > -1) {
				value = value.substring(0, value.length - 1);
			}

			connectionString += util.format('%s=%s;', key, value);
		});

		config.connectionString = connectionString;
		
		return config;
	}

	function execQuery (collectionName, query, data, cb) {
		if (_.isFunction(data)) {
			cb = data;
			data = null;
		}

		var config = getCollectionConfig(collectionName);

		sqlserver.open(config.connectionString, function (err, conn) {
			if (err) return cb(err);

			conn.queryRaw(query, data, function (err, results) {
				if (err && err.code === 1205) {
					return execQuery(collectionName, query, data, cb);
				}

				cb(err, results);
			});
		});
	}

	return adapter;
})();
