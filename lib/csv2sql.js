'use strict';

const through2 = require('through2');
const File = require('vinyl');
const _ = require('lodash');
const csvParser = require('babyparse');

const SQL_TYPE_INSERT = 1;
const SQL_TYPE_UPDATE = 2;
const SQL_TYPE_DELETE = 3;

const defaults = {
  sqlType: SQL_TYPE_INSERT,
  truncateTable: true,
  dropTableIfExist: false,
  resetAutoIncrement: false
};

module.exports = function(opts) {
  return through2.obj(function(file, enc, callback) {

    let options = _.assign({}, defaults, opts);
    let content = null;
    if (file.isBuffer()) {
      content = file.contents.toString();
    }
    if (file.isStream()) {
      console.log('stream is currently unsupported');
      return callback();
    }
    if (file.isNull()) {
      console.log('file is empty');
      return callback(null, file);
    }

    let data = null;
    if (content != null) {
      try {
        data = csvParser.parse(content).data;
      } catch (e) {
        console.error(e);
        throw new Error('Failed to parse csv data');
      }
    }

    if (data == null) {
      return callback();
    }
    let tableName = file.basename.split('.')[0];
    let rawFields = data.shift();
    let fields = rawFields.map(function(field) {
      return `\`${field}\``;
    });
    data = data.filter(function(row) {
      let columns = row.filter(function (column) {
        return column !== '';
      });
      return columns.length !== 0 && row.length === fields.length;
    });
    if (data.length === 0) {
      return callback();
    }

    let SQL = '';
    switch (options.sqlType) {
      case SQL_TYPE_DELETE:
        SQL = generateDeleteSQL(tableName, rawFields[0], data, options);
        break;
      case SQL_TYPE_UPDATE:
        SQL = generateUpdateSQL(tableName, fields, data, options);
        break;
      case SQL_TYPE_INSERT:
      default:
        SQL = generateInsertSQL(tableName, fields, data, options);
        break;
    }

    if (SQL === '') {
      return callback();
    }
    let sqlFile = new File({
      cwd: file.cwd,
      base: file.cwd,
      path: `${file.dirname}/${tableName}.sql`,
      contents: new Buffer(SQL)
    });
    this.push(sqlFile);
    return callback();
  });
};


const generateInsertSQL = function (tableName, fields, data, options) {

  let insertRows = data.map(function (row) {
    let columns = row.map(function(column, idx) {
      let numberLike = Number(column);
      if (Number.isInteger(numberLike)) {
        return numberLike;
      } else if (!Number.isNaN(numberLike) && numberLike % 1 !== 0) {
        // return float val
        // ref. http://stackoverflow.com/questions/3885817/how-do-i-check-that-a-number-is-float-or-integer
        return numberLike;
      } else {
        return `'${column}'`;
      }
    });
    return `(${columns.join(',')})`;
  });
  if (insertRows.length === 0) {
    return '';
  }

  let values = insertRows.join(',');
  if (values.length === 0) {
    return '';
  }

  let optionalStatement = '';
  if (options.dropTableIfExist) {
    optionalStatement = `
      DROP TABLE IF EXISTS \`${tableName}\`;
    `
  } else if (options.truncateTable) {
    optionalStatement = `
      TRUNCATE \`${tableName}\`;
    `
  }

  return `
    ${optionalStatement}
    INSERT INTO \`${tableName}\` (${fields})
    VALUES
      ${values}
    ;
  `;

};

const generateUpdateSQL = function (tableName, data, options) {
  return ``;
};

const generateDeleteSQL = function (tableName, deleteByField, data, options) {

  let ids = [];
  data.forEach(function(row, idx, arr) {
    ids.push(row[0]);
  });

  let inClause = ids.join(','); 

  let optionalStatement = '';
  if (options.resetAutoIncrement)
  {
    optionalStatement = `ALTER TABLE \`${tableName}\` AUTO_INCREMENT = 1;`;
  }
  return `
    DELETE FROM
      \`${tableName}\`
    WHERE
      \`${deleteByField}\`
    IN
      (${inClause})
    ;
    ${optionalStatement}
  `;

};