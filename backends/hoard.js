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

  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var sets = metrics.sets;
  var pctThreshold = metrics.pctThreshold;
  
  var stats= [];

  for (key in counters) {
    var value = counters[key];
    var valuePerSecond = value / (flushInterval / 1000); // calculate "per second" rate

    stats.push(['stats.' + key, valuePerSecond]);
    stats.push(['stats_counts.' + key, value]);

    numStats += 1;
  }

  for (key in timers) {
    if (timers[key].length > 0) {
      var values = timers[key].sort(function (a,b) { return a-b; });
      var count = values.length;
      var min = values[0];
      var max = values[count - 1];

      var cumulativeValues = [min];
      for (var i = 1; i < count; i++) {
          cumulativeValues.push(values[i] + cumulativeValues[i-1]);
      }

      var sum = min;
      var mean = min;
      var maxAtThreshold = max;


      var key2;

      for (key2 in pctThreshold) {
        var pct = pctThreshold[key2];
        if (count > 1) {
          var thresholdIndex = Math.round(((100 - pct) / 100) * count);
          var numInThreshold = count - thresholdIndex;

          maxAtThreshold = values[numInThreshold - 1];
          sum = cumulativeValues[numInThreshold - 1];
          mean = sum / numInThreshold;
        }

        var clean_pct = '' + pct;
        clean_pct.replace('.', '_');
        stats.push(['stats.timers.' + key + '.mean_'  + clean_pct, mean]);
        stats.push(['stats.timers.' + key + '.upper_' + clean_pct, maxAtThreshold]);
        stats.push(['stats.timers.' + key + '.sum_'   + clean_pct, sum]);
      }

      sum = cumulativeValues[count-1];
      mean = sum / count;

      var sumOfDiffs = 0;
      for (var i = 0; i < count; i++) {
         sumOfDiffs += (values[i] - mean) * (values[i] - mean);
      }
      var stddev = Math.sqrt(sumOfDiffs / count);
      stats.push(['stats.timers.' + key + '.std', stddev]);
      stats.push(['stats.timers.' + key + '.upper', max]);
      stats.push(['stats.timers.' + key + '.lower', min]);
      stats.push(['stats.timers.' + key + '.count', count]);
      stats.push(['stats.timers.' + key + '.sum', sum]);
      stats.push(['stats.timers.' + key + '.mean', mean]);
      
      numStats += 1;
    }
  }

  for (key in gauges) {
    stats.push(['stats.gauges.' + key, gauges[key]]);
    numStats += 1;
  }

  for (key in sets) {
    stats.push(['stats.sets.' + key + '.count',sets[key].values().length]);
    numStats += 1;
  }

  stats.push(['statsd.numStats',  numStats]);
  stats.push(['stats.statsd.graphiteStats.calculationtime', (Date.now() - starttime)]);
  updateStats( stats, ts  );
};

var known_whisper_files= {};
var whisper_file_check_queue= [];
var processing_whisper_queue= false;

function whisper_queue_item_processed( cb, err, filename ) {
  cb( err, filename);
  known_whisper_files[ filename ] = true;
  processing_whisper_queue= false;
  if( whisper_file_check_queue.length > 0 ) process.nextTick( process_whisper_file_queue );
}


function process_whisper_file_queue() {
    if( whisper_file_check_queue.length > 0 ) {
       processing_whisper_queue= true;
       var filenameCb= whisper_file_check_queue.shift();
       var filename= filenameCb[0];
       var cb= filenameCb[1];
       var measure= filenameCb[2];
       
       // If the whisper file was added since this entry was added to the queue, then just call back now.
       if( known_whisper_files[ filename ]) {
          whisper_queue_item_processed( cb, err, filename);
       } else {
          fs.exists( filename, function (exists) {
            if( exists ) whisper_queue_item_processed( cb, null, filename);
            else {
                mkdirp( path.dirname(filename),0777, function(err) {
                    if( err ) { 
                        console.log("Bad news, couldn't create folder", err ); 
                        whisper_queue_item_processed( cb, err, filename)
                    }
                    else {
                        var aggregation= get_aggregation_for_measure( measure );
                        var storage_schema= get_storage_schema_for_measure( measure );
                        if( storage_schema == null || aggregation == null ) {
                            var err= new Error( "Problem finding Storage Schema / Aggregation info for: "+ measure +"; "+ JSON.stringify(storage_schema) +","+ JSON.stringify(aggregation));
                            whisper_queue_item_processed( cb, err, filename)
                        }
                        else {
                            console.log( "Creating '"+ measure+ "' using schema : '" + storage_schema.name +"' and aggregation: '"+aggregation.name+"'");
                        }
                        hoard.create( filename, storage_schema.retentions, aggregation.xFilesFactor, aggregation.aggregationMethod, function(err) {
                            if (err) console.log( err ) ;
                            whisper_queue_item_processed( cb, err, filename)
                        });
                    }
                });
            }
        });
      }
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
    if( known_whisper_files[ filename ]) {
        cb(null, filename);
    }
    else {
        whisper_file_check_queue.push( [filename,cb, measure] );
        process.nextTick( process_whisper_file_queue );
    }
} 

function updateStats( stats, ts ) {
  var statsRemaining= stats.length;
  var startTs= new Date().getTime();
  for(var i=0;i < stats.length; i++) {
    (function( stat ) {
      ensure_whisper_file( stat[0], function( err, filename ) {
          if( err ) console.log( err )
          else {
            //TODO: deal with errors...
            hoard.update( filename, stat[1], ts, function(err) {
                    //TODO: sort out callbacks and error ahndling properly.
                    if( err ) console.log( ts +":" + filename,  err );
//                    else console.log( ts +" : " + filename + ' updated');
                    
                    statsRemaining--;
                    if( statsRemaining == 0 ) {
                        console.log('All Hoard files updated, in ' + (new Date().getTime() - startTs) +"ms");
                    }
            });
          }
      });
    })( stats[i] );
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
  return true;
};
