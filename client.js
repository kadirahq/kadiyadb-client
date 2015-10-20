var transport = require('kadiyadb-transport');
var protocol = require('kadiyadb-protocol');
var Transport = transport.Transport;

function Client() {
	this._id = 0;
	this._inflight = {};
	this.__read = this._read.bind(this);
}

// `connect` connects the Client to a kadiyadb server
// callback is passed a posible error. Connection is successful if err is null
Client.prototype.connect = function(url, callback) {
	var self = this;

	transport.dial(url, function(err, connection){
		if(err){
			callback(err)
			return
		}

		self._transport = new Transport(connection);
		callback(null);

		self.__read();
	})
};

Client.prototype._call = function(type, encoder, requests, callback) {
	var self = this;

	if(!self._transport){
		callback("Not connected")
		return
	}

	var batch = []
	requests.forEach(function(req){
		var item = new encoder(req).encode().toBuffer();
		batch.push(item);
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
				case 0:
					var item = protocol.ReqTrack.decode(res);
					break;
				case 1:
					var item = protocol.ReqFetch.decode(res);
					break;
				default:
					var item = null;
			}

      resBatch.push(item);
    });

		callback(resBatch);
		self.__read();
	});
}

Client.prototype.track = function(requests, callback) {
	this._call(0, protocol.ReqTrack, requests, callback);
}

Client.prototype.fetch = function(requests, callback) {
	this._call(1, protocol.ReqFetch, requests, callback);
};

module.exports = Client;
