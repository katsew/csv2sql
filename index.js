'use strict';

const vfs = require('vinyl-fs');
const csv2sql = require('./lib/csv2sql.js');

module.exports = function (src, dest) {
  try {
    vfs.src(src)
      .pipe(csv2sql({
        src: src,
        dest: dest
      }))
      .pipe(vfs.dest(dest));
  } catch (e) {
    console.log(e);
  }
};