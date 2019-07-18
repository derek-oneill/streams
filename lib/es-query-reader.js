'use strict';

var util = require('util');
var stream = require('stream');
var _ = require('lodash');

function EsQueryReader(client, index, type, query, bufferSize){
    stream.Readable.call(this, {objectMode: true});

    this.client = client;
    this.index = index;
    this.type = type;
    this.query = query;
    this.size = bufferSize || 500;
    this.buf = [];
    this.loading = false;
    this.total = 0;
    this.scroll_id = undefined;
}

util.inherits(EsQueryReader, stream.Readable);

EsQueryReader.prototype.nextBatch = function() {
    var self = this;

    return this.client.search({
        index: this.index,
        type: this.type,
        search_type: 'scan',
        scroll: '1m',
        scroll_id: this.scroll_id,
        body: _.merge({}, this.query, { size: this.size })
    })
        .then(function (result) {
            self.buf = _.pluck(result.hits.hits, '_source');
            self.scroll_id = _.pluck(result, ['_scroll_id']);
            self.loading = false;
        })
        .catch(function(err){
            self.loading = false;
            self.emit('error', err);
        });
};

EsQueryReader.prototype._read = function () {
    var self = this;

    if (this.buf.length > 0) return this.push(this.buf.shift());

    if (this.loading) return;

    this.nextBatch()
        .then(function () {
            if (self.buf.length > 0) {
                self.push(self.buf.shift());
            } else {
                self.push(null);
            }
        });
};

EsQueryReader.prototype.destroy = function () {
    this.loading = false;
    this.buf = [];
    this.emit('close');
};

module.exports.EsQueryReader = EsQueryReader;