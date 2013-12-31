var _ = require('underscore');
var util = require('util');

var utils = module.exports = {};

utils.castFromSql = function (schema, values) {
  var _values = _.clone(values);

  Object.keys(values).forEach(function (key) {
    
    if (!schema[key]) return;

    var type = schema[key].type;
    if (!type) return;

    _values[key] = utils.castValueFromSql(type, _values[key]);
  });

  return _values;
};

utils.castValueFromSql = function (type, value) {
  if(type === 'array' || type === 'json') {
    try {
      value = JSON.parse(value);
    }
    catch(e) {
      return;
    }
  }

  if(type === 'boolean') {
    var val = value;

    if(val === 0) value = false;
    if(val === 1) value = true;
  }

  return value;
};

utils.prepareValue = function(value) {
  if(!value) return value;

  if (_.isFunction(value)) {
    value = value.toString();
  }

  if (Array.isArray(value) || value.constructor && value.constructor.name === 'Object') {
    try {
      value = JSON.stringify(value);
    } catch (e) {
      value = value;
    }
  }

  return value;
};

utils.buildSelectStatement = function(criteria, table) {
  var query = '';

  if(criteria.groupBy || criteria.sum || criteria.average || criteria.min || criteria.max) {
    query = 'SELECT ';

    if(criteria.limit){
        query += 'TOP ' + criteria.limit + ' ';
    }

    // Append groupBy columns to select statement
    if(criteria.groupBy) {
      if(criteria.groupBy instanceof Array) {
        criteria.groupBy.forEach(function(opt){
          query += opt + ', ';
        });

      } else {
        query += criteria.groupBy + ', ';
      }
    }

    // Handle SUM
    if (criteria.sum) {
      if(criteria.sum instanceof Array) {
        criteria.sum.forEach(function(opt){
          query += 'SUM(' + opt + ') AS ' + opt + ', ';
        });

      } else {
        query += 'SUM(' + criteria.sum + ') AS ' + criteria.sum + ', ';
      }
    }

    // Handle AVG (casting to float to fix percision with trailing zeros)
    if (criteria.average) {
      if(criteria.average instanceof Array) {
        criteria.average.forEach(function(opt){
          query += 'AVG(' + opt + ') AS ' + opt + ', ';
        });

      } else {
        query += 'AVG(' + criteria.average + ') AS ' + criteria.average + ', ';
      }
    }

    // Handle MAX
    if (criteria.max) {
      if(criteria.max instanceof Array) {
        criteria.max.forEach(function(opt){
          query += 'MAX(' + opt + ') AS ' + opt + ', ';
        });

      } else {
        query += 'MAX(' + criteria.max + ') AS ' + criteria.max + ', ';
      }
    }

    // Handle MIN
    if (criteria.min) {
      if(criteria.min instanceof Array) {
        criteria.min.forEach(function(opt){
          query += 'MIN(' + opt + ') AS ' + opt + ', ';
        });

      } else {
        query += 'MIN(' + criteria.min + ') AS ' + criteria.min + ', ';
      }
    }

    // trim trailing comma
    query = query.slice(0, -2) + ' ';

    // Add FROM clause
    return query += 'FROM ' + table + ' ';
  }

  // Else select ALL
  return util.format('SELECT * FROM [%s] ', table);
};
