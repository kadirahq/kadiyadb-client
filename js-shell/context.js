var vm = require('vm');

function create(options) {
  var client = options.client;
  var database = null;

  var context = {};

  context.exit = function() {
    console.log('Bye!');
    process.exit(0);
  };

  context.pretty = function(val) {
    console.log(JSON.stringify(val, null, 2));
  };

  context.use = function(_database) {
    database = _database;
  }

  context.track = function(req) {
    if(!req.database) {
      req.database = database;
    }

    return client.trackFuture(req).wait();
  };

  context.track.help = function() {
    console.log('HELP_TRACK');
  };

  context.track.example = function() {
    var now = Date.now() * 1e6;

    return {
      database: database || "kadiyadb",
      time: now,
      total: 100,
      count: 10,
      fields: ['a', 'b', 'c', 'd'],
    };
  };

  context.fetch = function(req) {
    if(!req.database) {
      req.database = database;
    }

    return client.fetchFuture(req).wait();
  };

  context.fetch.help = function() {
    console.log('HELP_FETCH');
  };

  context.fetch.example = function() {
    var now = Date.now() * 1e6;

    return {
      database: database || "kadiyadb",
      from: now - 3.6*1e9,
      to: now,
      fields: ['a', 'b', 'c', 'd'],
    };
  };

  return vm.createContext(context);
}

module.exports = {
  create: create,
};
