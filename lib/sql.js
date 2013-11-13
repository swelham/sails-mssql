var _ = require('underscore');
_.str = require('underscore.string');
var utils = require('./utils');
var util = require('util');

var sql = {

  normalizeSchema: function (schema) {
    return _.reduce(schema, function(memo, field) {

      var attrName = field.Field;

      memo[attrName] = {
        defaultsTo: field.Default,
        // TODO: autoIncrement
        //autoIncrement: field.autoIncrement
      };

      if(field.primaryKey) {
        memo[attrName].primaryKey = field.primaryKey;
      }

      if(field.unique) {
        memo[attrName].unique = field.unique;
      }

      if(field.indexed) {
        memo[attrName].indexed = field.indexed;
      }

      return memo;
    }, {});
  },

  addColumn: function (collectionName, attrName, attrDef) {
    var columnDefinition = sql._schema(collectionName, attrDef, attrName);

    return util.format('ALTER TABLE [%s] ADD %s', collectionName, columnDefinition);
  },

  removeColumn: function (collectionName, attrName) {
    return util.format('ALTER TABLE [%s] DROP COLUMN %s', collectionName, attrName);
  },

  describeTableQuery: function (collectionName) {
    return util.format(
      'SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT ' +
      'FROM INFORMATION_SCHEMA.COLUMNS ' +
      'WHERE TABLE_NAME = \'%s\'',
      collectionName);
  },

  describeIndexQuery: function (collectionName) {
    return util.format(
        'SELECT COLUMN_NAME, CONSTRAINT_TYPE ' +
        'FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc ' +
            'JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu ON tc.CONSTRAINT_NAME = ccu.Constraint_name ' +
        'WHERE tc.TABLE_NAME = \'%s\'',
        collectionName);
  },

  selectQuery: function (collectionName, options) {
    var tableName = util.format('[%s]', collectionName);
    var query = utils.buildSelectStatement(options, collectionName);
    var criteria = sql.serializeOptions(collectionName, options);

    return query + criteria;
  },

  insertQuery: function (collectionName, data) {
    return util.format('INSERT INTO [%s] (%s) OUTPUT INSERTED.* VALUES (%s)',
      collectionName,
      sql.attributes(collectionName, data),
      sql.values(collectionName, data));
  },

  deleteQuery: function (collectionName, options) {
    return util.format('DELETE FROM [%s] %s',
      collectionName,
      sql.serializeOptions(collectionName, options));
  },

  schema: function(collectionName, attributes) {
    return sql.build(collectionName, attributes, sql._schema);
  },

  _schema: function(collectionName, attribute, attrName) {
    var type = sqlTypeCast(attribute.type);

    if(attribute.primaryKey) {

      if(type === 'INT') {
	return util.format('[%s] [INT] IDENTITY(1,1) NOT NULL CONSTRAINT PK_%s_ID PRIMARY KEY', attrName, collectionName);
      }

      return util.format('[%s] NVARCHAR(MAX) NOT NULL', attrName);
    }

    if(attribute.unique) {
      return util.format('[%s] %s NOT NULL UNIQUE', attrName, type);
    }

    return util.format('[%s] %s', attrName, type);
  },

  attributes: function(collectionName, attributes) {
    return sql.build(collectionName, attributes, sql.prepareAttribute);
  },

  values: function(collectionName, values, key) {
    return sql.build(collectionName, values, sql.prepareValue, ', ', key);
  },

  updateCriteria: function(collectionName, values) {
    var query = sql.build(collectionName, values, sql.prepareCriterion);
    query = query.replace(/IS NULL/g, '=NULL');
    return query;
  },

  prepareCriterion: function(collectionName, value, key, parentKey) {
    if (validSubAttrCriteria(value)) {
      return sql.where(collectionName, value, null, key);

    }

    var attrStr, valueStr;

    if (parentKey) {
      attrStr = sql.prepareAttribute(collectionName, value, parentKey);
      valueStr = sql.prepareValue(collectionName, value, parentKey);

      var nakedButClean = _.str.trim(valueStr,'\'');

      if (key === '<' || key === 'lessThan') return attrStr + '<' + valueStr;
      else if (key === '<=' || key === 'lessThanOrEqual') return attrStr + '<=' + valueStr;
      else if (key === '>' || key === 'greaterThan') return attrStr + '>' + valueStr;
      else if (key === '>=' || key === 'greaterThanOrEqual') return attrStr + '>=' + valueStr;
      else if (key === '!' || key === 'not') {
        if (value === null) return attrStr + 'IS NOT NULL';
        else return attrStr + '<>' + valueStr;
      }
      else if (key === 'like') return attrStr + ' LIKE \'' + nakedButClean + '\'';
      else if (key === 'contains') return attrStr + ' LIKE \'%' + nakedButClean + '%\'';
      else if (key === 'startsWith') return attrStr + ' LIKE \'' + nakedButClean + '%\'';
      else if (key === 'endsWith') return attrStr + ' LIKE \'%' + nakedButClean + '\'';
      else throw new Error('Unknown comparator: ' + key);
    } else {
      attrStr = sql.prepareAttribute(collectionName, value, key);
      valueStr = sql.prepareValue(collectionName, value, key);

      if (_.isNull(value)) {
        return attrStr + " IS NULL";
      } else return attrStr + "=" + valueStr;
    }
  },

  prepareValue: function(collectionName, value, attrName) {

    if (_.isDate(value)) {
      value = toSqlDate(value);
    }

    if (_.isFunction(value)) {
      value = value.toString();
    }

    return wrapInQuotes(value);
  },

  prepareAttribute: function(collectionName, value, attrName) {
    return util.format('[%s]', attrName);
  },

  where: function(collectionName, where, key, parentKey) {
    return sql.build(collectionName, where, sql.predicate, ' AND ', undefined, parentKey);
  },

  predicate: function(collectionName, criterion, key, parentKey) {
    var queryPart = '';

    if (parentKey) {
      return sql.prepareCriterion(collectionName, criterion, key, parentKey);
    }

    // OR
    if (key.toLowerCase() === 'or') {
      queryPart = sql.build(collectionName, criterion, sql.where, ' OR ');
      return ' ( ' + queryPart + ' ) ';
    }

    // AND
    else if (key.toLowerCase() === 'and') {
      queryPart = sql.build(collectionName, criterion, sql.where, ' AND ');
      return ' ( ' + queryPart + ' ) ';
    }

    // IN
    else if (_.isArray(criterion)) {
      queryPart = sql.prepareAttribute(collectionName, null, key) + " IN (" + sql.values(collectionName, criterion, key) + ")";
      return queryPart;
    }

    // LIKE
    else if (key.toLowerCase() === 'like') {
      return sql.build(collectionName, criterion, function(collectionName, value, attrName) {
        var attrStr = sql.prepareAttribute(collectionName, value, attrName);

        if (_.isRegExp(value)) {
          throw new Error('RegExp LIKE criterias not supported by the MSSQLAdapter.');
        }

        var valueStr = sql.prepareValue(collectionName, value, attrName);

        // Handle escaped percent (%) signs [encoded as %%%]
        valueStr = valueStr.replace(/%%%/g, '\\%');

        return attrStr + " LIKE " + valueStr;
      }, ' AND ');
    }

    // NOT
    else if (key.toLowerCase() === 'not') {
      throw new Error('NOT not supported yet!');
    }

    // Basic criteria item
    else {
      return sql.prepareCriterion(collectionName, criterion, key);
    }
  },

  serializeOptions: function(collectionName, options) {
    var queryPart = '';

    if (options.where) {
      queryPart += 'WHERE ' + sql.where(collectionName, options.where) + ' ';
    }

    if (options.sort) {
      queryPart += 'ORDER BY ';

      _.each(options.sort, function(direction, attrName) {

        queryPart += sql.prepareAttribute(collectionName, null, attrName) + ' ';

        if (direction === 1) {
          queryPart += 'ASC, ';
        } else {
          queryPart += 'DESC, ';
        }
      });

      if(queryPart.slice(-2) === ', ') {
        queryPart = queryPart.slice(0, -2) + ' ';
      }
    }

    if (options.groupBy) {
      queryPart += 'GROUP BY ';

      if(!Array.isArray(options.groupBy)) options.groupBy = [options.groupBy];

      options.groupBy.forEach(function(key) {
        queryPart += key + ', ';
      });

      queryPart = queryPart.slice(0, -2) + ' ';
    }

    // TODO: implement mssql version of skip and take
    /*if (options.limit) {
      queryPart += 'LIMIT ' + options.limit + ' ';
    } else {
      // Some MySQL hackery here.  For details, see:
      // http://stackoverflow.com/questions/255517/mysql-offset-infinite-rows
      queryPart += 'LIMIT 18446744073709551610 ';
    }

    if (options.skip) {
      queryPart += 'OFFSET ' + options.skip + ' ';
    }*/

    return queryPart;
  },

  build: function(collectionName, collection, fn, separator, keyOverride, parentKey) {
    separator = separator || ', ';
    var $sql = '';

    _.each(collection, function(value, key) {
      $sql += fn(collectionName, value, keyOverride || key, parentKey);

      $sql += separator;
    });

    return _.str.rtrim($sql, separator);
  }
};

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

function wrapInQuotes(value) {
  return util.format('\'%s\'', value);
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

function validSubAttrCriteria(c) {
  return _.isObject(c) && (
  !_.isUndefined(c.not) || !_.isUndefined(c.greaterThan) || !_.isUndefined(c.lessThan) ||
  !_.isUndefined(c.greaterThanOrEqual) || !_.isUndefined(c.lessThanOrEqual) || !_.isUndefined(c['<']) ||
  !_.isUndefined(c['<=']) || !_.isUndefined(c['!']) || !_.isUndefined(c['>']) || !_.isUndefined(c['>=']) ||
  !_.isUndefined(c.startsWith) || !_.isUndefined(c.endsWith) || !_.isUndefined(c.contains) || !_.isUndefined(c.like));
}

module.exports = sql;
