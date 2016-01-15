var Redis = require('redis');

var cache = {
  install: function (mongoose, options) {
    var cacheClient = Redis.createClient(options);
    var self = this;

    var log = options.debug ? console.log : function () {
    };

    //save native exec functions
    mongoose.Query.prototype.old_execFind = mongoose.Query.prototype.execFind;
    mongoose.Query.prototype.old_exec = mongoose.Query.prototype.exec;
    mongoose.Aggregate.prototype.old_exec = mongoose.Aggregate.prototype.exec;

    //set a new method on Query and Aggregate proto
    mongoose.Query.prototype.cache = function(key, expire) {
      this.__cached = true;
      this.__cacheKey = key || genKey(this);
      this.__expire = expire || options.default_expire || false;
      return this;
    };
    mongoose.Aggregate.prototype.cache = function(key) {
      this.__cached = true;
      this.__cacheKey = key || genKey(this);
      return this;
    };

    //define a new exec function using cache or native function if cache not enabled or not entry found
    function cacheExec(caller, args) {
      //if cache not enabled for this query/aggregation, use native function
      if (!this.__cached) {
        return this['old_'+caller].apply(this, args);
      }
      var self = this;

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
            }
            cacheClient.set(self.__cacheKey, JSON.stringify(result), function(err, writeStatus){
              if(err || writeStatus != 'OK'){
                console.log(err || "write failed");
              }

              //set expire time if defined
              if(self.__expire !== false){
                cacheClient.expire(self.__cacheKey, self.__expire);
              }

              for (var arg in args){
                if(args.hasOwnProperty(arg) && typeof args[arg] == 'function'){
                  args[arg](null, result);
                }
              }
            });
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
  return JSON.stringify({
    model: query.model.modelName,
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
  return JSON.stringify({
    model: aggregate._model.modelName,
    pipeline: aggregate._pipeline,
    options: aggregate.options
  });
}

module.exports = cache;