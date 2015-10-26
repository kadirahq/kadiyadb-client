var Protocol = require('kadiyadb-protocol').protocol;
var Transport = require('kadiyadb-transport');

function connect(url, callback) {
  var client = new Connection();

  Transport.connect(url, function(err, conn) {
    if (err) {
      callback(err);
      return;
    }

    client._conn = conn;
    callback(null, client);
    client._read();
  });
}

function Connection() {
  this._conn = null;
  this._inflight = {};
  this._nextId = 0;
}

Object.assign(Connection.prototype, {
  ERRNOCONN: new Error("not connected"),
});

Connection.prototype.track = function(req, callback) {
  if (!this._conn || !this._conn.connected) {
    callback(this.ERRNOCONN);
    return;
  }

  req = {
    id: this._nextId++,
    track: req,
  };

  console.log('req', req);
  this._inflight[req.id] = callback;
  this._conn.send(Protocol.Request, req);
};

Connection.prototype.trackBatch = function(reqs, callback) {
  var todo = reqs.length;
  var errs = [];
  var ress = [];

  for (var i = 0; i < reqs.length; i++) {
    (function(i) {
      this.track(reqs[i], function(err, res) {
        errs[i] = err;
        ress[i] = res;

        if (--todo == 0) {
          callback(errs, ress);
        }
      });
    })();
  }
};

Connection.prototype.fetch = function(req, callback) {
  if (!this._conn || !this._conn.connected) {
    callback(this.ERRNOCONN);
    return;
  }

  req = {
    id: this._nextId++,
    fetch: req,
  };

  this._inflight[req.id] = callback;
  this._conn.send(Protocol.Request, req);
};

Connection.prototype.fetchBatch = function(reqs, callback) {
  var todo = reqs.length;
  var errs = [];
  var ress = [];

  for (var i = 0; i < reqs.length; i++) {
    (function(i) {
      this.fetch(reqs[i], function(err, res) {
        errs[i] = err;
        ress[i] = res;

        if (--todo == 0) {
          callback(errs, ress);
        }
      });
    })();
  }
};

Connection.prototype._read = function() {
  var self = this;

  this._conn.recv(Protocol.Response, function(err, res) {
    console.log('c:res', res);
    var id = res.id || 0;
    var cb = self._inflight[id];

    if (!cb) {
      return
    }

    if (res.track) {
      cb(null, res.track);
    } else if (res.fetch) {
      cb(null, res.fetch);
    }

    self._read();
  });
};

module.exports = {
  Connection: Connection,
  connect: connect,
};
