var Client = require('../../');

var ADDRESS = "kdb://localhost:8000";
var BATCH_SIZE = 100;
var CONCURRENCY = 10;

var counter = 0;
var client = new Client();

client.connect(ADDRESS, function (err, conn) {
  var reqs = [];
  for(var i=0; i<BATCH_SIZE; i++){
		reqs.push({
      database: 'test',
  		time: 0,
  		total: 45.6,
  		count: 5,
  		fields: ['foo', 'bar']
    })
	}

  function track() {
    client.track(reqs, function () {
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
