
var util = require('util'),
	_ = require('underscore');

_.str = require('underscore.string');

function toSqlDate(date) {

  date = date.getUTCFullYear() + '-' +
    ('00' + (date.getUTCMonth()+1)).slice(-2) + '-' +
    ('00' + date.getUTCDate()).slice(-2) + ' ' +
    ('00' + date.getUTCHours()).slice(-2) + ':' +
    ('00' + date.getUTCMinutes()).slice(-2) + ':' +
    ('00' + date.getUTCSeconds()).slice(-2);

  return date;
}

var sql = {

	insertQuery: function (tableName, values) {
		var queryHeader = 'DECLARE @ID INT;';

		queryHeader += 'DECLARE @PARAMS NVARCHAR(MAX) = N\'@IDOUT int OUTPUT,';

		Object.keys(values).forEach(function (key) {
			queryHeader += util.format('@%s NVARCHAR(MAX),', key);
		});

		queryHeader = _.str.rtrim(queryHeader, ',') + '\';';

		var query = 'INSERT INTO [' + tableName + '] (' + sql.attributes(values) + ') VALUES (' + sql.params(values) + ') SELECT @IDOUT = @@IDENTITY';

		return queryHeader + 'EXEC sp_executesql N\'' + query + '\', @PARAMS, @IDOUT = @ID OUTPUT,' + sql.paramValues(values) + ' SELECT @ID';
	},

	attributes: function (values) {
		return sql.build(values, function (key, value) {
			return util.format('[%s],', key);
		});
	},

	params: function (values) {
		return sql.build(values, function (key, value) {
			return util.format('@%s,', key);
		});
	},

	paramValues: function (values) {
		return sql.build(values, function (key, value) {
			return util.format('@%s = \'%s\',', key, sql.prepareValue(values[key]));
		});
	},

	prepareValue: function (value) {
		if (_.isDate(value)) {
			value = toSqlDate(value);
		}

		if (_.isFunction(value)) {
			value = value.toString();
		}

		return value;
	},

	build: function (values, fn) {
		var _values = '';
		
		Object.keys(values).forEach(function (key) {
			_values += fn(key, values[key]);
		});

		return _.str.rtrim(_values, ',');
	}

};

module.exports = sql;