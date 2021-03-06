var Client = require('../../');

var ADDRESS = "kdb://localhost:8000";
var CONCURRENCY = 1000;
var counter = 0;

setInterval(function() {
  console.log(counter / 10);
  counter = 0;
}, 10000);

Client.connect(ADDRESS, function(err, conn) {
  var req = {
    database: 'test',
    time: Date.now() * 1e6,
    total: 2,
    count: 1,
    fields: ['a', 'b', 'c']
  };

  function track() {
    conn.track(req, function(err) {
      if (err) {
        setTimeout(track, 500);
        return
      }

      counter++;
      track();
    });
  }

  for (var i = 0; i < CONCURRENCY; i++) {
    track();
  }
});
