// todo: look at refactoring out parts of this file into modules

var _ = require('underscore');
_.str = require('underscore.string');
var util = require('util');

function buildStatement (key, options, sqlQuery) {
	if (!sqlQuery) {
		sqlQuery = '';
	}

	return sqlQuery += builders[key](options);
}

function build (options, cb, separator) {
	if (!separator) {
		if (separator === false) {
			separator = '';
		}
		else {
			separator = ',';
		}
	}

	separator += ' ';

	var result = '';
	var data = options.data;

	Object.keys(data).forEach(function (attr) {
		result += util.format('%s %s', cb(attr, options), separator);
	});

	return _.str.rtrim(result, separator);
}

function wrapSqlObjectName (attr, options) {
	return util.format('[%s]', attr);
}

function wrapSqlParameter (attr, options) {
	return '@' + attr;
}

function wrapSqlParameterValue (attr, options) {
	return util.format('DECLARE @%s %s = %s',
		attr,
		sqlTypeCast(options.definition[attr].type),
		prepareSqlValue(options.data[attr]));
}

function prepareSqlValue (value) {
	if (_.isDate(value)) {
		value = toSqlDate(value);
	}

	if (_.isFunction(value)) {
		value = value.toString();
	}

	value = sanitizeSql(value);

	return wrapInQuotes(value);
}

function wrapInQuotes(value) {
  return util.format('\'%s\'', value);
}

function sanitizeSql (value) {
	// todo: look at the full list of characters that need to be handled to prevent sql injection
	return value.replace(/\'/g, '\'\'');
}

function toSqlDate(date) {
	date = date.getUTCFullYear() + '-' +
		('00' + (date.getUTCMonth()+1)).slice(-2) + '-' +
		('00' + date.getUTCDate()).slice(-2) + ' ' +
		('00' + date.getUTCHours()).slice(-2) + ':' +
		('00' + date.getUTCMinutes()).slice(-2) + ':' +
		('00' + date.getUTCSeconds()).slice(-2);

	return date;
}

function sqlTypeCast(type) {
	type = type && type.toLowerCase();

	switch (type) {
		case 'string':
		case 'text':
		case 'array':
		case 'json':
			return 'NVARCHAR(MAX)';

		case 'boolean':
			return 'BIT';

		case 'int':
		case 'integer':
			return 'INT';

		case 'float':
		case 'double':
			return 'FLOAT';

		case 'date':
			return 'DATE';

		case 'datetime':
			return 'DATETIME';

		default:
			console.error("Unregistered type given: " + type);
			return "NVARCHAR(MAX)";
	}
}

var builders = {
	insert: function (options) {
		var query = build(options, wrapSqlParameterValue, false);

		query += util.format('INSERT INTO [%s] (%s) OUTPUT INSERTED.* VALUES (%s)',
			options.table,
			build(options, wrapSqlObjectName),
			build(options, wrapSqlParameter));

		return query;
	},

	/*update: function (options) {
		console.log('update:');
		console.dir(options);
		throw new Error('not implemented');
	},

	select: function (options) {
		console.log('select:');
		console.dir(options);
		throw new Error('not implemented');
	},

	delete: function (options) {
		console.log('delete:');
		console.dir(options);
		throw new Error('not implemented');
	}*/
};

var sql = {
	insertQuery: function (collection, data) {
		return buildStatement('insert', {
			table: collection.identity,
			definition: collection.definition,
			data: data
		});
	}
};

// todo: replace this with - module.exports = sql;
module.exports = _.extend(require('./sql_orig'), sql);

/* 
	Basic Queries:

	Create:
	INSERT INTO [pet] ([name], [createdAt], [updatedAt]) OUTPUT INSERTED.* VALUES ('ben', '2013-12-20 14:00:54', '2013-12-20 14:00:54')

	Update:
	UPDATE [pet] SET [name]='nivea', [updatedAt]='2013-12-20 14:02:13' WHERE [id]='4' 

	FindAll (need to test with other find options):
	SELECT * FROM [pet]

	FindOne:
	SELECT * FROM [pet] WHERE [id]='4'

	Delete:
	DELETE FROM [pet] WHERE [id]='4'

*/