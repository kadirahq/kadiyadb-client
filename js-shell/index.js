#!/usr/bin/env node

var vm = require('vm');
var repl = require('repl');
var Future = require('fibers/future');
var KadiyaDB = require('../');
var Context = require('./context');

Future.task(function() {
  var url = process.argv[2] || 'kdb://localhost:8000';

  var client = Future.wrap(KadiyaDB.connect)(url).wait();
  var shell = repl.start({
    prompt: "❯ ",
    ignoreUndefined: true,
    eval: fiberEval,
  });

  shell.context = Context.create({
    client: Future.wrap(client),
  });
}).detach();

function fiberEval(cmd, ctx, file, callback) {
  Future.task(function() {
    try {
      var res = vm.runInContext(cmd, ctx);
    } catch (e) {
      callback(e)
    }

    callback(null, res);
  }).detach();
}
