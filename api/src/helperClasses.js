const { BigQuery } = require('@google-cloud/bigquery');
const  Memcached = require('memcached');
const fs = require('fs');

class MissingDataError extends Error {
  constructor(message) {
    super(message);
    this.name = 'MissingDataError';
  }
}

class MalformedArgumentError extends Error {
  constructor(message) {
    super(message);
    this.name = "MalformedArgumentError";
  }
}

class BigQueryParser {
  constructor(mapping){
    this.mapping = mapping;
  }

  formatRowsToJson (rows) {
    let resObj = rows.map((row) => {
      return {
        attribution_url: row.attribution_id,
        app_id: row.app_package_name,
        ordered_id: row.event_timestamp,
        user: {
          id: row.user_pseudo_id,
          metadata: {
            continent: row.geo.continent,
            country: row.geo.country,
            region: row.geo.region,
            city: row.geo.city,
          },
          ad_attribution: {
            source: this.getSource(row.attribution_id),
            data: {
              advertising_id: row.device.advertising_id,
            },
          },
        },
        event: {
          name: this.parseName(row.action),
          date: row.event_date,
          timestamp: row.event_timestamp,
          value_type: this.getValueType(row.label),
          value: this.getValue(row.label) || row.val,
          level: this.getLevel(row.screen) || this.getLevel(row.label)||this.getLevel(row.action)||"0",
          profile: this.getProfile(row.screen) || 'unknown',
          rawData: {
            action: row.action,
            label: row.label,
            screen: row.screen,
            value: row.value,
          }
        },
      };
    });
    return resObj;
  }

  getLevel(input) {
    try {
      if(input.split('-').length > 1) {
        return input.split('-')[0].split(' ')[1];
      } else if (input.split('_').length > 1) {
        let arr = input.split('_');
        let val = arr[arr.length-1];
        if(Number(val)){
          return val;
        }
      } else {
        return null;
      }
    } catch(e) {
      return null;
    }
  }

  getProfile(screen) {
    try {
      return screen.split(':')[1].trim();
    } catch(e) {
      return null;
    }
  }

  parseName(action) {
    if(action.indexOf('Segment') !== -1 || action.indexOf('Level') !== -1 || action.indexOf('Monster') !==-1) {
      return action.split('_')[0].trim();
    } else {
      return action;
    }
  }

  getValueType(label) {
    let spacesSplit = label.split(' ');
    if (spacesSplit[0] === 'Puzzle') {
      return label.split(':')[0].replace('Puzzle ', '');
    } else if (spacesSplit[1] === 'puzzles') {
      return spacesSplit[1];
    } else if (spacesSplit[0] === 'days_since_last') {
      return 'days';
    } else if (spacesSplit[0] === 'total_playtime' || spacesSplit[0] === 'average_session') {
      return 'seconds';
    } else if (spacesSplit[0].indexOf('Level') !== -1){
      return 'Monster Level'
    }else{
      return null;
    }
  }

  getValue(label) {
    if(label.indexOf('Puzzle') !== -1) {
      return label.split(':')[1].trim();
    } else if (label.indexOf('puzzles') != -1) {
      return label.split(' ')[0].trim();
    } else {
      return null;
    }
  }

  getSource(referralString) {
    if (!referralString) return "no-source";
    const group = referralString.split('_');
    let source = group[0].toString().toLowerCase();
    if(this.mapping[source])
    {
      return this.mapping[source];
    }
    return 'no-source';
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
    this.bq.createQueryJob(this.queryOptions).then((response) => {
      const job = response[0]
      this.jobId = job.id;
      if(job) {
        const queryResultOptions = {
          maxResults: this.MAXROWS,
          autopaginate: false,
          timeoutMs: 60000
        };
        return job.getQueryResults(queryResultOptions, this.paginationCallback.bind(this));
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
        this.onComplete(this.rows, this.jobId, this.token);
      }
    } else {
      this.allResultsFetched = true;
      if (this.onComplete) {
        this.onComplete(this.rows, null, null);
      }
    }
  }

  fetchNext(onCompleteCallback) {
    if(!this.jobId || this.allResultsFetched) {
      if(onCompleteCallback) {
        onCompleteCallback ([], null, null);
      }
      return;
    }
    const job = this.bq.job(this.jobId);
    if(onCompleteCallback) {
      this.setCompleteCallback(onCompleteCallback);
    }
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

//currently unused, keeping it around in case we need it
class MemcachedManager {
  constructor(address) {
    if(!address) {
      throw new Error('Please provide a cache address');
    }
  console.log(`memcached endpoint: ${address}`);
    this.memcached = new Memcached(address);
  }
  createKey (prefix, params) {
    if(typeof(prefix) !== 'string' && typeof(prefix) !== 'number') {
      throw new Error("Keys can only be made with strings or numbers!");
    }
    let key = `__${prefix}__`;
    for(let param in params) {
      if (params[param]) {
        if (typeof(param) !== 'string' && typeof(param) !== 'number') {
          throw new Error("Keys can only be made with strings or numbers!");
        }
        key += params[param].toString();
      }
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

class SqlLoader {
  constructor (paths, basePath = '.', encoding = 'utf-8') {
    if(!paths) {
      throw new Error('you must provide an array or map of paths to sql files');
    }
    this.basePath = basePath;
    this.fileStrings = {};
    this.encoding = encoding;
    this.loadPaths(paths);
  }

  loadPaths(paths) {
    if(Array.isArray(paths)) {
      this.loadPathsFromArray(paths);
    } else if (typeof(paths) === 'object') {
      this.loadPathsFromMap(paths);
    }
  }

  loadPathsFromArray(paths) {
    try {
      paths.forEach((path) => {
        const filePath = `${this.basePath}/${path}`;
        const file = fs.readFileSync(filePath, this.encoding);
        this.fileStrings[path] = file.toString();
      });
    } catch (e) {
      console.error('error loading one or more sql files');
      throw e;
    }
  }

  loadPathsFromMap(paths) {
    try {
      for (let path in paths) {
        const filePath = `${this.basePath}/${paths[path]}`;
        const file = fs.readFileSync(filePath, this.encoding);
        this.fileStrings[paths[path]] = file.toString();
      }
    } catch (e) {
      console.error('error loading one or more sql files');
      throw e
    }
  }

  getQueryString(path) {
    if(!this.fileStrings[path]) throw new Error(`no query for path ${path}!`);
    return this.fileStrings[path];
  }
}

module.exports = {
  MissingDataError,
  MalformedArgumentError,
  BigQueryManager,
  BigQueryParser,
  MemcachedManager,
  SqlLoader
};
