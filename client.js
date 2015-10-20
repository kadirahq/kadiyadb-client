var Protocol = require('kadiyadb-protocol').protocol;
var Transport = require('kadiyadb-transport');

function connect(url, callback) {
	var client = new Client();

	Transport.connect(url, function(err, conn) {
		if(err) {
			callback(err);
			return;
		}

		client._transport = conn;
		callback(null, client);
		client._read();
	});
}

function Client() {
	this._id = 0;
	this._inflight = {};
}

Object.assign(Client.prototype, {
	ERRNOCONN: new Error("not connected"),
	TYPETRACK: 1,
	TYPEFETCH: 2,
});

Client.prototype._call = function(type, requests, callback) {
	var self = this;

	if(!Array.isArray(requests)) {
		requests = [requests];
	}

	if(!self._transport){
		callback(this.ERRNOCONN);
		return;
	}

	var batch = []
	requests.forEach(function(req){
		switch (type) {
			case self.TYPETRACK:
				var item = Protocol.ReqTrack.encode(req);
				break;
			case self.TYPEFETCH:
				var item = Protocol.ReqFetch.encode(req);
				break;
			default:
				console.error('unknown request type', type);
		}

		batch.push(item.toBuffer());
	})

	var batchId = self._id++;
	self._inflight[batchId] = callback;
	self._transport.WriteBatch(batchId, type, batch);
};

Client.prototype._read = function (argument) {
	var self = this;

	self._transport.ReadBatch(function(id, type, batch){
		var callback = self._inflight[id];
		if(!callback) {
			console.error("unknown message id");
			return;
		}

		delete self._inflight[id];

		var resBatch = [];
    batch.forEach(function(res){
			switch (type) {
				case self.TYPETRACK:
					var item = Protocol.ResTrack.decode(res);
					break;
				case self.TYPEFETCH:
					var item = Protocol.ResFetch.decode(res);
					break;
				default:
					console.error('unknown response type', type);
			}

      resBatch.push(item);
    });

		callback(null, resBatch);
		self._read();
	});
}

Client.prototype.track = function(requests, callback) {
	this._call(this.TYPETRACK, requests, callback);
}

Client.prototype.fetch = function(requests, callback) {
	this._call(this.TYPEFETCH, requests, callback);
};

module.exports = {
	Client: Client,
	connect: connect,
};
