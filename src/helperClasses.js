const { BigQuery } = require('@google-cloud/bigquery');
const { Memcached } = require('memcached');

class MissingArgumentError extends Error {
  constructor(message) {
    super(message)
    this.name = 'MissingArgumentError';
  }
}

class MalformedArgumentError extends Error {
  constructor(message) {
    super(message);
    this.name = "MalformedArgumentError";
  }
}

class BigQueryManager {
  constructor (queryOptions, maxRows, jobId, token) {
    this.queryOptions = queryOptions;
    this.MAXROWS = maxRows;
    this.rows = [];
    this.bq = new BigQuery();
    this.allResultsFetched = false;
    if(jobId) {
      this.jobId = jobId;
    }
    if (token) {
      this.token = token;
    }
  }

  async start (onCompleteCallback) {
    this.setCompleteCallback(onCompleteCallback);
    return this.bq.createQueryJob(this.queryOptions).then((response) => {
      const job = response[0]
      this.jobId = job.id;
      if(job) {
        const queryResultOptions = {
          maxResults: this.MAXROWS,
          autopaginate: false,
        };
        job.getQueryResults(queryResultOptions, this.paginationCallback.bind(this));
      }
    }).catch((err)=> {
      console.error(err);
      throw err;
    });
  }

  paginationCallback(err, rows, nextQuery, apiResponse) {
    if (err) throw err;
    this.rows = rows;
    if(nextQuery) {
      this.token = nextQuery.pageToken;
      if(this.onComplete) {
        this.onComplete(this.rows, this.jobId, this.token, this.allResultsFetched);
      }
    } else {
      this.allResultsFetched = true;
      if (this.onComplete) {
        this.onComplete(this.rows, null, null, this.allResultsFetched);
      }
    }
  }

  fetchNext(onCompleteCallback) {
    if(!this.jobId || this.allResultsFetched) {
      if(onCompleteCallback) {
        onCompleteCallback ([], null, null, true);
      }
      return;
    }
    const job = this.bq.job(this.jobId);
    this.setCompleteCallback(onCompleteCallback);
    const queryOptions = {
      maxResults: this.MAXROWS,
      autopaginate: false,
      pageToken: this.token,
    };
    try {
      job.getQueryResults(queryOptions, this.paginationCallback.bind(this));
    } catch (e) {
      throw e;
    }
  }

  setCompleteCallback(callback) {
    this.onComplete = callback;
  }
  isComplete() {
    return this.allResultsFetched;
  }
  getOptions() {
    return this.queryOptions;
  }
}

class MemcachedManager {
  constructor(address) {
    if(!address) {
      throw new Error('Please provide a cache address');
    }
    this.memcached = new Memcached(address);
  }
  createKey (prefix, params) {
    if(typeof(prefix) !== 'string' && typeof(prefix) !== 'number') {
      throw new Error("Keys can only be made with strings or numbers!");
    }
    let key = `__${prefix}__`;
    for(let param in params) {
      if (typeof(param) !== 'string' && typeof(param) !== 'number') {
        throw new Error("Keys can only be made with strings or numbers!");
      }
      key += params[param].toString();
    }
    return key;
  }
  cacheResults (key, data, duration) {
    this.memcached.set(key, data, duration, function (err) {
      if(err) {
        console.error(err);
        throw err;
      }
    });
  }

  removeCache(key) {
    this.memcached.del(key, (err) => {
      if(err) {
        console.error(err);
        throw err;
      }
    });
  }

  get(key, callback) {
    this.memcached.get(key, callback);
  }

  set (key, data, callback) {
    this.memcached.set(key, data, callback);
  }
}

module.exports = {
  MalformedArgumentError,
  BigQueryManager,
  MemcachedManager
};
