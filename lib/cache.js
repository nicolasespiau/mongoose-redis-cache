var Redis = require('redis');
var async = require('async');

var cache = {
  install: function (mongoose, options) {
    var cacheClient = Redis.createClient(options);

    /**
     * Allow to delete all redis entries by key according to the given pattern
     * @param wildcard
     * @param callbackDW
     */
    cacheClient.delete_wildcard = function (wildcard, callbackDW) {
      var self = this;
      var total = 0;
      self.keys(wildcard, function (err, keys) {
        if (err) {
          return callbackDW(err);
        }

        async.each(
          keys,
          function (key, callback) {
            self.del(key, function (err, delCount) {
              if (err) {
                console.log(err);
                callback(err);
              }else{
                total += delCount;
                callback();
              }
            });
          },
          function () {
            return callbackDW(null, keys.length, total);
          }
        );
      });
    };

    var self = this;

    var log = options.debug ? console.log : function () {
    };

    //save native exec functions
    mongoose.Query.prototype.old_execFind = mongoose.Query.prototype.execFind;
    mongoose.Query.prototype.old_exec = mongoose.Query.prototype.exec;
    mongoose.Aggregate.prototype.old_exec = mongoose.Aggregate.prototype.exec;

    //set a new method on Query and Aggregate proto
    mongoose.Query.prototype.cache = mongoose.Aggregate.prototype.cache = function _cache(key, expire) {
      this.__cached = true;
      this.__cacheKey = key;
      this.__expire = expire || options.default_expire || false;
      return this;
    };

    //define a new exec function using cache or native function if cache not enabled or not entry found
    function cacheExec(caller, args) {
      //if cache not enabled for this query/aggregation, use native function
      if (!this.__cached) {
        return this['old_'+caller].apply(this, args);
      }
      var self = this;

      this.__cacheKey = this.__cacheKey || genKey(this);
      //try to get entry in cache for the current key
      cacheClient.get(self.__cacheKey, function (err, obj) {
        var i;

        //if entry found then call callback and return current object
        if (obj) {
          log('cache hit: ', self.__cacheKey);
          for (var arg in args){
            if(args.hasOwnProperty(arg) && typeof args[arg] == 'function'){
              args[arg](null, JSON.parse(obj));
            }
          }
          return self;
        } else { //if no entry found, then call native function and store result in cache
          self["old_"+caller].call(self, function(err, result){
            if(err){
              console.log(err);

              for (var arg in args) {
                if (args.hasOwnProperty(arg) && typeof args[arg] == 'function') {
                  args[arg](null, result);
                }
              }
            }else {
              cacheClient.set(self.__cacheKey, JSON.stringify(result), function (err, writeStatus) {
                if (err || writeStatus != 'OK') {
                  console.log(err || "write failed");
                }

                //set expire time if defined
                if (self.__expire !== false) {
                  cacheClient.expire(self.__cacheKey, self.__expire);
                }

                for (var arg in args) {
                  if (args.hasOwnProperty(arg) && typeof args[arg] == 'function') {
                    args[arg](null, result);
                  }
                }
              });
            }
          });
        }
      });
    }

    //override exec methods on Query and Aggregate to use the ones using cache
    mongoose.Query.prototype.execFind = function (arg1, arg2) {
      return cacheExec.call(this, 'execFind', arguments);
    };
    mongoose.Query.prototype.exec = function (arg1, arg2) {
      return cacheExec.call(this, 'exec', arguments);
    };
    mongoose.Aggregate.prototype.exec = function (arg1, arg2) {
      return cacheExec.call(this, 'exec', arguments);
    };

    /**
     * New method on mongoose Model to execute a findOne using cache
     * @param options
     * @param callbackGetOne
     */
    mongoose.Model.getOne = function(options, callbackGetOne){
      this.findOne(options.filters || {})
        .select(options.select || {})
        .skip(options.skip || {})
        .sort(options.sort || {})
        .cache()
        .exec(function(err, result){
          return callbackGetOne(err, result);
        });
    };

    /**
     * New method on mongoose Model to execute a find using cache
     * @param options
     * @param callbackGetMany
     */
    mongoose.Model.getMany = function(options, callbackGetMany){
      this.find(options.filters || {})
        .select(options.select || {})
        .skip(options.skip || {})
        .sort(options.sort || {})
        .limit(options.limit || {})
        .cache()
        .exec(function(err, result){
          return callbackGetMany(err, result);
        });
    };

    /**
     * New method on mongoose Model to flush specific cache
     * @param callback
     */
    mongoose.Model.flushCache = function(callback){
      if(typeof callback != 'function'){
        callback = options.debug ? console.log : function(){};
      }
      cacheClient.delete_wildcard(this.modelName+"_*", function(err, status){
        return callback(err, status);
      });
    };

    return mongoose;
  }
};

/**
 * generate cache key based on query params
 * @param query
 * @returns {*}
 */
function genKey(query) {
  if (query._pipeline) {
    return genKeyAggregate(query);
  }
  return query.model.modelName+"_"+JSON.stringify({
      query: query._conditions,
      fields: query._fields,
      options: query.options
    });
}

/**
 * generate cache key based on aggregate params
 * @param aggregate
 */
function genKeyAggregate(aggregate) {
  return aggregate._model.modelName+"_"+JSON.stringify({
      pipeline: aggregate._pipeline,
      options: aggregate.options
    });
}

module.exports = cache;