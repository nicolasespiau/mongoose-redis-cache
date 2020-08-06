const Redis = require('ioredis');
let cacheClient;
RegExp.prototype.toJSON = RegExp.prototype.toString;

function isJson(test) {
  try {
    const o = JSON.parse(test);

    // Handle non-exception-throwing cases:
    // Neither JSON.parse(false) or JSON.parse(1234) throw errors, hence the type-checking,
    // but... JSON.parse(null) returns null, and typeof null === "object",
    // so we must check for that, too. Thankfully, null is false, so this suffices:
    return !!o && typeof o === "object";
  }
  catch (e) {
    return false;
  }
}

const cache = {
  install(mongoose, options) {
    //save native exec functions
    if (!mongoose.Query.prototype.directExec)
      mongoose.Query.prototype.directExec = mongoose.Query.prototype.exec;
    if (!mongoose.Aggregate.prototype.directExec)
      mongoose.Aggregate.prototype.directExec = mongoose.Aggregate.prototype.exec;

    mongoose.Query.prototype.cacheExec = mongoose.Aggregate.prototype.cacheExec = cacheExec;

    cacheClient = new Redis(options);

    cacheClient.on("connect", async () => {
      console.info("Redis connected");
      await this.init(mongoose, options);
    });

    cacheClient.on("error", async err => {
      console.error("Redis connect error:", err);
      this.reset(mongoose, options);
      cacheClient.end(true, options);
    });

    cacheClient.on("end", async () => {
      console.info("Redis connection ended");
      this.reset(mongoose, options);
    });
  },

  init(mongoose, options) {
    /**
     * Allow to delete all redis entries by key according to the given pattern
     * @param wildcard
     */
    cacheClient.delete_wildcard = async wildcard => {
      let total = 0;
      let keys;
      try {
        keys = await this.keys(wildcard);
      } catch(e) {
        //TODO
      }

      try {
        total += await this.del(keys.join(" "));
      } catch(e) {
        //TODO
      }
      
      return total;
    };

    const log = options.debug ? console.log : function () {
    };

    //set a new method on Query and Aggregate proto
    mongoose.Query.prototype.cache = mongoose.Aggregate.prototype.cache = function _cache(key, expire) {
      this.__cached = true;
      this.__cacheKey = key;
      this.__expire = expire || options.default_expire || false;
      return this;
    };

    //define a new exec function using cache or native function if cache not enabled or not entry found

    //override exec methods on Query and Aggregate to use the ones using cache
    mongoose.Query.prototype.exec = () => {
      if (this.__cached) return this.cacheExec(...arguments)
      return this.directExec(...arguments);
    };
    mongoose.Aggregate.prototype.exec =  () => {
      if (this.__cached) return this.cacheExec(...arguments)
      return this.directExec(...arguments);
    };

    /**
     * New method on mongoose Model to flush specific cache
     * @param callback
     */
    mongoose.Model.flushCache = async () => {
      return await cacheClient.delete_wildcard(this.modelName + "_*")
    };

    mongoose.connection.emit('mongooseup');
    return mongoose;
  },
  reset: function (mongoose, options) {
    //restore original methods
    mongoose.Query.prototype.exec = mongoose.Query.prototype.directExec;
    mongoose.Aggregate.prototype.exec = mongoose.Aggregate.prototype.directExec;

    //set a neutral method on Query and Aggregate proto
    mongoose.Query.prototype.cache = mongoose.Aggregate.prototype.cache = (key, expire) => {
      return this;
    };

    mongoose.Model.flushCache = () => {};
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
  return query.model.modelName + "_" + JSON.stringify({
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
  return aggregate._model.modelName + "_" + JSON.stringify({
      pipeline: aggregate._pipeline,
      options: aggregate.options
    });
}

module.exports = cache;


async function cacheExec() {
  this.__cacheKey = this.__cacheKey || genKey(this);

  let result;
  //try to get entry in cache for the current key
  try {
    result = await cacheClient.get(this.__cacheKey);
  } catch (e) {
    //TODO LOG

    //read in cache failed, lets read in db
    return this.execDirect(...arguments);
  }

  if (!result || result === "" || !isJson(result)) { //if we found an empty string or nothing in cache then call native function and store result in cache
    result = this.execDirect(...arguments);

    try {
      cacheClient.set(this.__cacheKey, JSON.stringify(result));
    } catch (e) {
      //TODO
    }
    if (this.__expire !== false) cacheClient.expire(this.__cacheKey, this.__expire);
  }

  return result;
}