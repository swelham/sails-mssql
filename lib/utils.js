
var _ = require('underscore');

module.exports = {

	prepareValue: function (value) {
		if (!value) return value;
		if (_.isFunction(value)) value = value.toString();
		
		if (Array.isArray(value) || value.constructor && value.constructor.name === 'Object') {
			try {
				value = JSON.stringify(value);
			}
			catch (e) {
				value = value;
			}
		}

		return value;
	},

	castFromSql: function (schema, values) {
		var _values = _.clone(values);

		Object.keys(values).forEach(function (key) {
			
			if (!schema[key]) return;

			var type = schema[key].type;
			if (!type) return;

			if(type === 'array' || type === 'json') {
				try {
					_values[key] = JSON.parse(values[key]);
				}
				catch(e) {
					return;
				}
			}

			if(type === 'boolean') {
				var val = values[key];

				if(val === 0) _values[key] = false;
				if(val === 1) _values[key] = true;
			}
		});

		return _values;
	}

};