var Client = require('../../');

var ADDRESS = "kdb://localhost:8000";
var BATCH_SIZE = 100;
var CONCURRENCY = 10;
var counter = 0;

Client.connect(ADDRESS, function (err, conn) {
  var reqs = [];
  for(var i=0; i<BATCH_SIZE; i++){
		reqs.push({
      database: 'test',
  		time: i*60000000000,
  		total: 3.14,
  		count: 1,
  		fields: ['foo', 'bar']
    })
	}

  function track() {
    conn.track(reqs, function () {
      counter++;
      track();
    });
  }

  for(var i=0; i<CONCURRENCY; i++) {
    track();
  }
});

setInterval(function () {
  console.log(BATCH_SIZE*counter);
  counter = 0;
}, 1000);
