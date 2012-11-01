/*
 * Flush stats to graphite (http://graphite.wikidot.com/).
 *
 * To enable this backend, include 'graphite' in the backends
 * configuration array:
 *
 *   backends: ['graphite']
 *
 * This backend supports the following config options:
 *
 *   graphiteHost: Hostname of graphite server.
 *   graphitePort: Port to contact graphite server at.
 */

var hoard = require('hoard'), 
   fs = require('fs'),
   mkdirp= require('mkdirp'),
   net = require('net'),
   path = require('path'),
   util = require('util');

var debug;
var flushInterval;
var graphiteHost;
var graphitePort;
var schemas;
var aggregations;

var graphiteStats = {};

var post_stats = function graphite_post_stats(statString) {
  var last_flush = graphiteStats.last_flush || 0;
  var last_exception = graphiteStats.last_exception || 0;
  if (graphiteHost) {
    try {
      var graphite = net.createConnection(graphitePort, graphiteHost);
      graphite.addListener('error', function(connectionException){
        if (debug) {
          util.log(connectionException);
        }
      });
      graphite.on('connect', function() {
        var ts = Math.round(new Date().getTime() / 1000);
        statString += 'stats.statsd.graphiteStats.last_exception ' + last_exception + ' ' + ts + "\n";
        statString += 'stats.statsd.graphiteStats.last_flush ' + last_flush + ' ' + ts + "\n";
        this.write(statString);
        this.end();
        graphiteStats.last_flush = Math.round(new Date().getTime() / 1000);
      });
    } catch(e){
      if (debug) {
        util.log(e);
      }
      graphiteStats.last_exception = Math.round(new Date().getTime() / 1000);
    }
  }
}

var flush_stats = function graphite_flush(ts, metrics) {
  var starttime = Date.now();
  var statString = '';
  var numStats = 0;
  var key;
  var timer_data_key;
  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;
  var counter_rates = metrics.counter_rates;
  var timer_data = metrics.timer_data;
  var statsd_metrics = metrics.statsd_metrics;

  var stats=[];
  for (key in counters) {
    stats.push(['stats.'        + key, counter_rates[key]]);
    stats.push(['stats_counts.' + key, counters[key]]);

    numStats += 1;
  }

  for (key in timer_data) {
    if (Object.keys(timer_data).length > 0) {
      for (timer_data_key in timer_data[key]) {
         stats.push(['stats.timers.' + key + '.' + timer_data_key, timer_data[key][timer_data_key]]);
      }

      numStats += 1;
    }
  }

  for (key in gauges) {
    stats.push(['stats.gauges.' + key, gauges[key]]);

    numStats += 1;
  }

  for (key in sets) {
    stats.push(['stats.sets.' + key + '.count', sets[key].values().length]);

    numStats += 1;
  }

  for (key in statsd_metrics) {
    stats.push(['stats.statsd.' + key, statsd_metrics[key]]);
  }

  stats.push(['statsd.numStats',  numStats]);
  stats.push(['stats.statsd.graphiteStats.calculationtime', (Date.now() - starttime)]);

  updateStats( stats, ts  );
};

var known_whisper_files= {};
function process_whisper_file_queue(filename, cb, measure) {
   if( known_whisper_files[ measure ] && known_whisper_files[ measure ].created) {
      cb(null, filename);
   } else {
      fs.exists( filename, function (exists) {
        if( exists ) {
            known_whisper_files[ measure ].created= true;
            cb( null, filename);
        }
        else {
            mkdirp( path.dirname(filename),0777, function(err) {
                if( err ) { 
                    console.log("Bad news, couldn't create folder", err ); 
                    cb(err);
                }
                else {
                    var aggregation= get_aggregation_for_measure( measure );
                    var storage_schema= get_storage_schema_for_measure( measure );
                    if( storage_schema == null || aggregation == null ) {
                        var err= new Error( "Problem finding Storage Schema / Aggregation info for: "+ measure +"; "+ JSON.stringify(storage_schema) +","+ JSON.stringify(aggregation));
                        cb(err);
                    }
                    else {
                        console.log( "Creating '"+ measure+ "' using schema : '" + storage_schema.name +"' and aggregation: '"+aggregation.name+"'");
                        hoard.create( filename, storage_schema.retentions, aggregation.xFilesFactor, aggregation.aggregationMethod, function(err) {
                            if (err) console.log( err ) ;
                            known_whisper_files[ measure ].created= true;
                            cb(err, filename);
                        });
                    }
                }
            });
        }
    });
  }
}

function get_storage_schema_for_measure( measure ) {
    for(var i=0;i< schemas.length;i++ ){
        if( schemas[i].pattern.test( measure ) ) return schemas[i];
    }
    return null;
}

function get_aggregation_for_measure( measure ) {
    for(var i=0;i< aggregations.length;i++ ){
        if( aggregations[i].pattern.test( measure ) ) return aggregations[i];
    }
    return null;
}

function ensure_whisper_file( measure, cb) {
    var filename= "wsp_data" + path.sep + measure.replace(/\./g, path.sep);
    filename += ".wsp";
    process_whisper_file_queue( filename, cb, measure );
} 

function updateStats( stats, ts ) {
  var statsRemaining= stats.length;
  var originalStatsCount= statsRemaining;
  var startTs= new Date().getTime();
  for(var stat in stats) {
    var wspr;
    wspr= known_whisper_files[ stats[stat][0] ];
    if( !wspr ) {
        wspr= known_whisper_files[ stats[stat][0] ]= {values:[]};
    }
    wspr.values.push( [ts, stats[stat][1]] );
  }
}

var backend_status = function graphite_status(writeCb) {
  for (stat in graphiteStats) {
    writeCb(null, 'graphite', stat, graphiteStats[stat]);
  }
};

function parse_retentions( retentions_str) {
    var retentions= [];
    var archives= retentions_str.split(",");
    for( var i=0;i< archives.length;i++ ){
        var archive= archives[i].split(":");
        retentions[retentions.length]= [parseInt(archive[0]), parseInt(archive[1])];
    }
    return retentions;
}

var isFlushingStats= false;
function flushStats() {
    if( isFlushingStats ) {
        console.log("ignoring flush request");
        return;
    }
    else {
        isFlushingStats= true;
        var statsTocheck= [];
        var now = new Date().getTime();
        // This means that if a known whisper file comes 
        // in during a flush it will be ignored.
        var totalQueueSize= 0;
        for(var key in known_whisper_files) {
            if( known_whisper_files[key].values.length >0 ) {
                statsTocheck.push( key );
                totalQueueSize+= known_whisper_files[key].values.length;
            }
        }
        var metricsToFlushCount= statsTocheck.length
        if( metricsToFlushCount > 0 ) {
            totalQueueSize = totalQueueSize / metricsToFlushCount;
            var flush_whisper_file= function() {
                if( statsTocheck.length > 0 ) {
                    var nextMetric= statsTocheck.pop();
                     ensure_whisper_file( nextMetric, function( err, filename ) {
                              if( err ) {
                                console.log( err )
                                process.nextTick( flush_whisper_file );
                              }
                              else {
                                hoard.updateMany( filename, known_whisper_files[nextMetric].values, function(err) {
                                    if( err ) console.log( ts +":" + filename,  err );
                                    else {
                                        known_whisper_files[nextMetric].values= [];
                                    }
                                    process.nextTick( flush_whisper_file );
                                });
                              /* hoard.update( filename, known_whisper_files[nextMetric].values[0][1], known_whisper_files[nextMetric].values[0][0], function(err) {
                                    if( err ) console.log( ts +":" + filename,  err );
                                    else {
                                        known_whisper_files[nextMetric].values= [];
                                    }
                                    process.nextTick( flush_whisper_file );
                                });*/
                              }
                          });
                } else {
                    console.log( "Flushed all metrics ("+metricsToFlushCount+") in " + (new Date().getTime() - now ) +"ms [Average Length: " + totalQueueSize + "]");
                    isFlushingStats= false;
                }
            }
            flush_whisper_file();
        }
        else {
            isFlushingStats= false;
        }
    }
}

exports.init = function graphite_init(startup_time, config, events) {
  debug = config.debug;
  graphiteHost = config.graphiteHost;
  graphitePort = config.graphitePort;

  graphiteStats.last_flush = startup_time;
  graphiteStats.last_exception = startup_time;

  if( config.hoard && config.hoard.schemas ) {
    schemas= config.hoard.schemas;
  } 
  else {
    schemas= [{name: "default", pattern: /^stats.*/, retentions: "10:2160,60:10080,600:105192"}];
  }
  if( config.hoard && config.hoard.aggregations ) {
    aggregations= config.hoard.aggregations;
  }
  else {
    aggregations= [{name: "default", pattern: /.*/, xFilesFactor: 0.3, aggregationMethod: "average"}];
  }
  
  // Convert the given retentions string format to the internal hoard-compatible format
  for(var i=0;i< schemas.length;i++) {
    var schema= schemas[i];
    schema.retentions= parse_retentions( schema.retentions );
  }
  flushInterval = config.flushInterval;

  events.on('flush', flush_stats);
//  events.on('status', backend_status);
/*    hoard.create('users.hoard', [[1, 60], [10, 600]], 0.5, function(err) {
        if (err) throw err;
        console.log('Hoard file created!');
    });*/
  setInterval( flushStats, 10000 );
  return true;
};
