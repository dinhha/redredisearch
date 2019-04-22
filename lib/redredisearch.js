/*!
 * redredisearch
 * 
 * Forked from tj/reds
 * Original work Copyright(c) 2011 TJ Holowaychuk <tj@vision-media.ca>
 * Modified work Copyright(c) 2017 Kyle Davis
 * MIT Licensed
 */

/**
 * Module dependencies.
 */


var redis = require('redis');
const {SearchSchema, SearchIndexModel, Types} = require('./searchSchema');

function noop(){};

exports.enableDebug = false;

/**
 * Library version.
 */

exports.version = '0.0.1';

exports.SearchSchema = SearchSchema;

exports.Types = Types;

/**
 * Expose `Search`.
 */

exports.Search = Search;

/**
 * Expose `Query`.
 */

exports.Query = Query;

exports.Suggestion = Suggestion;

/**
 * Search types.
 */

var types = {
  intersect: 'and',
  union: 'or',
  and: 'and',
  or: 'or'
};

/**
 * Alternate way to set client 
 * provide your own behaviour.
 *
 * @param {RedisClient} inClient
 * @return {RedisClient}
 * @api public
 */

exports.setClient = function(inClient) {
  return exports.client = inClient;
}

/**
 * Create a redis client, override to
 * provide your own behaviour.
 *
 * @return {RedisClient}
 * @api public
 */

exports.createClient = function(){
  return exports.client
    || (exports.client = redis.createClient());
};

/**
 * Confirm the existence of the RediSearch Redis module
 *
 * @api public
 */

exports.confirmModule = function(cb) {
  exports.client.send_command('ft.create',[], function(err) {
    let strMsg = String(err);
    if (strMsg.indexOf('ERR wrong number of arguments') > 0) {
      cb(null);
    } else {
      cb(err);
    }
  });
}

/**
 * Return a new reds `Search` with the given `key`.
 * @param {String} key
 * @param {Object} opts
 * @return {Search}
 * @api public
 */

exports.createSearch = function(key,opts,cb){
  const 
    searchObj   = function(err,info) {
      if (err) { cb(err); } else {
        cb(err,new Search(key,info,opts));
      }
    };

  opts = !opts ? {} : opts;
  opts.payloadField = opts.payloadField ? opts.payloadField : 'payload';

  if (!key) throw new Error('createSearch() requires a redis key for namespacing');
  
  exports.client.send_command('FT.INFO',[key],function(err,info) {
    if (err) { 
      //if the index is not found, we need to make it.
      if (String(err).indexOf('Unknown Index name') > 0 ){
        let args = [
          key,
          'SCHEMA', opts.payloadField, 'text'
        ];
        exports.client.send_command(
          'FT.CREATE',
          args,
          function(err) {
            if (err) { cb(err); } else {
              exports.client.send_command('FT.INFO',[key],searchObj);
            }
          }
        );
      }

    } else { searchObj(err,info); }
  });
};


/**
 * @param {SearchSchema} searchSchema
 * @returns {Search} 
 */
exports.createSearchWithSchema = async function(searchSchema){
  const searchObj = function(err,info) {
      if (err) { 
        return Promise.reject(err);
      } else {
        return new Search(searchSchema.key, searchSchema);
      }
    };

  if (!searchSchema | !searchSchema.key | !searchSchema.defination) 
    throw new Error('createSearch() requires a redis key for namespacing');
  
  return new Promise((resolve, reject) => {
    exports.client.send_command('FT.INFO', [searchSchema.key], function(err,info) {
      if (err) { 
        //if the index is not found, we need to make it.
        if (String(err).indexOf('Unknown Index name') > 0 ){
          let args = [
            searchSchema.key,
            'SCHEMA',
            ...searchSchema.getFieldArgs()
          ];
          console.log('FT.CreateSearchIndex', args);

          exports.client.send_command(
            'FT.CREATE',
            args,
            function(err) {
              if (err) { 
                reject(err); 
              } else {
                exports.client.send_command('FT.INFO', [searchSchema.key], (err, info) => {
                  resolve(searchObj(err, info));
                });
              }
            }
          );
        } else {
          reject(err);
        }
  
      } else { 
        resolve(searchObj(err,info));
      }
    });

  });

};

/**
 * Return the words in `str`. This is for compatability reasons (convert OR queries to pipes)
 *
 * @param {String} str
 * @return {Array}
 * @api private
 */

exports.words = function(str){
  return String(str).match(/\w+/g);
};


/**
 * Initialize a new `Query` with the given `str`
 * and `search` instance.
 *
 * @param {String} str
 * @param {Search} search
 * @api public
 */

function Query(str, search) {
  this.str = str;
  this.type('and');
  this.search = search;
  this.filteres = {
    numeric: [],
    geo: {},
    tags: []
  };
}

/**
 * Set `type` to "union" or "intersect", aliased as
 * "or" and "and".
 *
 * @param {String} type
 * @return {Query} for chaining
 * @api public
 */

Query.prototype.type = function(type){
  if (type === 'direct') {
    this._directQuery = true;
  } else {
    this._direct = false;
    this._type = types[type];
  }
  return this;
};

/**
 * Limit search to the specified range of elements.
 *
 * @param {String} start
 * @param {String} stop
 * @return {Query} for chaining
 * @api public
 */
Query.prototype.between = function(start, stop){
  this._start = start;
  this._stop = stop;
  return this;
};

/**
 * Perform the query and callback `fn(err, ids)`.
 *
 * @param {Function} fn
 * @return {Query} for chaining
 * @api public
 */

Query.prototype.end = function(fn){
  var 
    key     = this.search.key,
    db      = this.search.client,
    query   = this.str,
    direct  = this._directQuery,
    args    = [],
    joiner  = ' ',
    rediSearchQuery;

  if (direct) {
    rediSearchQuery = query;
  } else {
    rediSearchQuery = exports.words(query);
    if (this._type === 'or') {
      joiner = '|'
    }
    rediSearchQuery = rediSearchQuery.join(joiner);
  }

  if (this.filteres.tags && this.filteres.tags.length > 0) {
    let tags = this.filteres.tags.map(tagsFilter => {
      if (tagsFilter.tags && tagsFilter.tags.length > 0) {
        return `@${tagsFilter.field}:{${tagsFilter.tags.join('|')}}`
      }
    });

    tags = tags.filter(t => t != null && t != '');

    if (tags.length > 0) {
      rediSearchQuery = `(${rediSearchQuery}) ${tags.join(' ')}`;
    }
  }
  
  args = [
    key,
    rediSearchQuery,
    'NOCONTENT'
  ];

  if (this.filteres.numeric.length > 0) {
    this.filteres.numeric.forEach(filter => {
      args.push(`FILTER ${filter.field} ${filter.min} ${filter.max}`);
    });
  }

  if (this.filteres.geo && this.filteres.geo.field) {
    const filter = this.filteres.geo;
    args.push(`GEOFILTER ${filter.field} ${filter.lng} ${filter.lat} ${filter.radius} ${filter.unit}`);
  }

  if (this.keysFilter && this.keysFilter.keys) {
    args.push(`INKEYS ${this.keysFilter.length} ${this.keysFilter.keys.join(',')}`);
  }

  if (this._start !== undefined) {
    args.push('LIMIT',this._start,this._stop);
  }

  debug("FT.Search");
  debug(args);

  db.send_command(
    'FT.SEARCH',
    args,
    function(err,resp) {
      if (err) { fn(err); } else {
        fn(err,resp.slice(1));
      }
    }
  );

  return this;
};

/**
 * @param {string} field
 * @param {number} min
 * @param {number} max
 * 
 * @returns {Query}
 */
Query.prototype.numericFilter = function(field, min, max) {
  this.filteres.numeric.push({
    field,
    min,
    max
  });

  return this;
};

/**
 * @param {string} field
 * @param {number} lat
 * @param {number} lng
 * @param {number} radius
 * @param {string} unit - m | km | ft | mi
 * 
 * @returns {Query}
 */
Query.prototype.geoFilter = function (field, lat, lng, radius, unit = 'ft') {
  this.filteres.geo = {
    field,
    lat,
    lng,
    radius,
    unit
  };

  return this;
};

/**
 * @param {string} field
 * @param {(string | [string])} tags
 */
Query.prototype.tagsFilter = function (field, tags) {
  if (typeof tags == 'string') {
    tags = [tags]
  }

  this.filteres.tags.push({
    field,
    tags
  });
  return this;
}

/**
 * @param {number} length
 * @param {[string]} keys
 * 
 * @returns {Query}
 */
Query.prototype.inKeys = function (length, keys) {
  this.keysFilter = {length, keys};
  return this;
};

/**
 * Initialize a new `Suggestion` with the given `key`.
 *  
 * @param {String} key
 * @param {Object} opts
 * @api public
 */
var Suggestion = function(key,opts) {
  this.key = key;
  this.client = exports.createClient();
  this.opts = opts || {};
  if (this.opts.fuzzy) {
    this.fuzzy = opts.fuzzy;
  }
  if (this.opts.maxResults) {
    this.maxResults = opts.maxResults;
  }
  if (this.opts.incr) {
    this.incr = opts.incr;
  }
  if (this.opts.withPayloads) {
    this.withPayloads = true;
  }
}

/**
 * Create a new Suggestion object
 * 
 * @param {String} key
 * @param {Object} opts
 * @api public 
 */
exports.suggestionList = function(key,opts) {
  return new Suggestion(key,opts);
}

/**
 * Set `fuzzy` on suggestion get. Can also be set via opts in the constructor
 *
 * @param {Boolean} isFuzzy
 * @return {Suggestion} for chaining
 * @api public
 */

Suggestion.prototype.fuzzy = function(isFuzzy){
  this.fuzzy = isFuzzy;
  return this;
};

/**
 * Set the max number of returned suggestions. Can also be set via opts in the constructor
 *
 * @param {Number} maxResults
 * @return {Suggestion} for chaining
 * @api public
 */

Suggestion.prototype.maxResults = function(maxResults){
  this.maxResults = maxResults;
  return this;
};

Suggestion.prototype.add = function(str, score, payload, fn) {
  if((typeof fn === 'undefined' || fn === null) && typeof payload === "function"){
    if(typeof fn !== 'undefined'){
      fn = payload;
    } else {
      var fn = payload;
    }
    payload = null;
  };

  var key = this.key;
  var db = this.client;
  var args = [
    key,
    str,
    score,
  ];
  if (this.incr) {
    args.push('INCR');
  }
  if(payload !== null){
    args.push('PAYLOAD', (typeof payload === 'object' ? JSON.stringify(payload) : payload.toString()));
  }
  db.send_command(
    'FT.SUGADD',
    args,
    fn || noop
  );
  return this;
}

Suggestion.prototype.get = function(prefix, fn) {
  var key = this.key;
  var db = this.client;
  var args = [
    key,
    prefix
  ];
  if (this.fuzzy) {
    args.push('FUZZY');
  }
  if (this.maxResults) {
    args.push('MAX',this.maxResults);
  }
  if (this.withPayloads) {
    args.push('WITHPAYLOADS');
  }

  db.send_command(
    'FT.SUGGET',
    args,
    fn
  );

  return this;
}

Suggestion.prototype.del = function(str,fn) {
  var key = this.key;
  var db = this.client;

  db.send_command(
    'FT.SUGDEL',
    [ 
      key,
      str
    ],
    fn
  );

  return this;
}

/**
 * Initialize a new `Search` with the given `key`.
 *
 * @param {String} key
 * @api public
 */

function Search(key,info,opts) {
  this.key = key;
  this.client = exports.createClient();
  this.opts = opts || {};
  this.info = info;
}

/**
 * Index the given `str` mapped to `id`.
 *
 * @param {String} str
 * @param {Number|String} id
 * @param {Function} fn
 * @api public
 */

Search.prototype.index = function(str, id, fn){
  var key = this.key;
  var db = this.client;
  var opts = this.opts;

  const args = [
    key,
    id,
    1,            //default - this should be to be set in future versions
    'NOSAVE',     //emulating Reds original behaviour
    'REPLACE',    //emulating Reds original behaviour
    'FIELDS',
    opts.payloadField,
    str
  ];

  debug("Search: add index");
  debug(args);


  db.send_command(
    'FT.ADD',
    args,
    fn || noop
  );

  return this;
};

/**
 * @param {string} id
 * @param {SearchIndexModel} model
 */
Search.prototype.indexModel = function(model, fn){
  var key = this.key;
  var db = this.client;

  const fields = model.getFieldArgs();
  const id = model.docId;
  const args = [
    key,
    id,
    1,            //default - this should be to be set in future versions
    'NOSAVE',     //emulating Reds original behaviour
    'REPLACE',    //emulating Reds original behaviour
    'FIELDS',
    ...fields
  ];

  debug("Search: add indexModel");
  debug(args);

  db.send_command(
    'FT.ADD',
    args,
    fn || noop
  );

  return this;
};

/**
 * Remove occurrences of `id` from the index.
 *
 * @param {Number|String} id
 * @api public
 */

Search.prototype.remove = function(id, fn){
  fn = fn || noop;
  var key = this.key;
  var db = this.client;
  
  //this.removeIndex(db, id, key, fn);
  db.send_command(
    'FT.DEL',
    [
      key,
      id
    ],
    fn
  )
  
  return this;
};

/**
 * Perform a search on the given `query` returning
 * a `Query` instance.
 *
 * @param {String} query
 * @param {Query}
 * @api public
 */

Search.prototype.query = function(query){
  return new Query(query, this);
};


debug = function (...args) {
  if (exports.enableDebug) {
    if (args && args.length == 1) {
      console.log(args[0]);
    } else {
      console.log(args);
    }
  }
}