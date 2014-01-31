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

var ceres = require('../../ceres'), 
   net = require('net');

var debug;
var graphiteHost;
var graphitePort;
var schemas;
var aggregations;
var tree;

var echoFileName= null;
var echoFs= null;

var graphiteStats = {};

var clampedTs= null;
var flushInterval;
var flush_stats = function graphite_flush(ts, metrics) {
  if( echoFs ) {
    var o= {"ts": ts, "clampedTs": clampedTs, "metrics": metrics};
    echoFs.write( JSON.stringify(o) +",\n");
  }
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
    stats.push(['stats.counters.' + key + ".rate", counter_rates[key]]);
    stats.push(['stats.counters.' + key + ".count", counters[key]]);

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

  stats.push(['stats.statsd.numStats',  numStats]);
  stats.push(['stats.statsd.ceres.calculationtime', (Date.now() - starttime)]);

  // Figure out clamped stuff.
  if( clampedTs == null ) {
    // Choose nearest interval edge.
    var difference= ( ts % flushInterval );
    if( difference < flushInterval /2 )  {
      clampedTs = ts - ( ts % flushInterval )
    }
    else {
      clampedTs = ts +  (flushInterval - ( ts % flushInterval ))
    }
  }
  else {
    clampedTs += flushInterval;
  }
  if( debug ) {
    if( clampedTs != ts) l.log( "Clamped: " + new Date(clampedTs *1000)+ " UnClamped: "+ new Date(ts*1000), "DEBUG" );
  }  
  updateStats( stats, clampedTs, true );
};

var known_whisper_files= {};
function process_whisper_file_queue(measure, cb) {
  
   if( known_whisper_files[ measure ] && known_whisper_files[ measure ].node) {
      cb(null, known_whisper_files[ measure ].node);
   } else {
     tree.getNode( measure, function(node) {
       if( node ) {
         known_whisper_files[ measure ].node= node;
         cb( null, node)
       } else {
         tree.createNode( measure, {timeStep: 60, aggregationMethod: 'avg'}, function(err, node) {
           if( err ) cb(err);
           else {
             known_whisper_files[ measure ].node= node;
             cb( null, node)
           }
         })
       }
     });
  }
}

function updateStats( stats, ts, flush ) {
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
  if( flush === true ) _flushStats();
}

var backend_status = function graphite_status(writeCb) {
  for (stat in graphiteStats) {
    writeCb(null, 'graphite', stat, graphiteStats[stat]);
  }
};

var isFlushingStats= false;
function _flushStats() {
    if( isFlushingStats ) {
      if( debug ) { l.log("ignoring overlapping flush request", "ERROR"); }
        return;
    }
    else {
        if( debug ) {
          l.log( "Flushing existing metrics", "DEBUG" );
        }
        isFlushingStats= true;
        var statsTocheck= [];
        var now = new Date().getTime();
        // This means that if a known whisper file comes 
        // in during a flush it will be ignored.
        var totalQueueSize= 0;
        for(var key in known_whisper_files) {
            if( known_whisper_files[key].values.length >0 ) {
              // We need to take a 'copy' of the data because, as the node.write maybe async
              // the known_whisper_files[key].values array could be updated by the outer statsd
              // code via #flush_stats before we've finished writing out the existing data values.
                var valuesToFlush= known_whisper_files[key].values.slice();
                statsTocheck.push( {"key":key, "values":valuesToFlush} );
                totalQueueSize+= valuesToFlush.length;

                // Set back to empty so any new data that comes in, is distinguishable from the sent.
                // if we have any problems with the write process, we'll just pop them on the front of the list
                // on-error.
                known_whisper_files[key].values= [];
            }
        }
        var metricsToFlushCount= statsTocheck.length
        if( metricsToFlushCount > 0 ) {
            totalQueueSize = totalQueueSize / metricsToFlushCount;
            var flush_whisper_file= function() {
                if( statsTocheck.length > 0 ) {
                    var nextMetric= statsTocheck.pop();
                     process_whisper_file_queue( nextMetric.key, function( err, node ) {
                              if( err ) {
                                if( debug ) { l.log("!!! DROPPED STAT", "ERROR"); l.log( err, "ERROR" ); }
                                process.nextTick( flush_whisper_file );
                              }
                              else {
                                node.write( nextMetric.values, function(err) {
                                  if( err ) {
                                    if( debug ) { 
                                      l.log("!!! DROPPED STAT", "ERROR");
                                      l.log( nextMetric, "ERROR" );
                                      l.log( err, "ERROR" ); 
                                    }
                                    // We will try the stats again (fingers-crossed this doesn't happen indefinitely!)
                                  }
                                  process.nextTick( flush_whisper_file );
                                });
                              }
                          });
                } else {
                    updateStats( [['stats.statsd.ceres.flushTime', (new Date().getTime() - now)]], Math.round(new Date().getTime() / 1000)  );
                    if( debug ) {
                      l.log( "Flushed all metrics ("+metricsToFlushCount+") in " + (new Date().getTime() - now ) +"ms [Average Length: " + totalQueueSize + "]");
                    }
                    isFlushingStats= false;
                }
            }
            process.nextTick( flush_whisper_file );
        }
        else {
            isFlushingStats= false;
        }
    }
}

exports.init = function graphite_init(startup_time, config, events, logger) {
  debug = config.debug;
  l = logger;
  echoFileName= config.echoFile;
  flushInterval = config.flushInterval/1000;
  graphiteHost = config.graphiteHost;
  graphitePort = config.graphitePort;

  graphiteStats.last_flush = startup_time;
  graphiteStats.last_exception = startup_time;

  function begin(t, msg) {
    if( debug ) { l.log( msg, "INFO" ); }
    if( echoFileName ) {
      echoFs= require('fs').createWriteStream( echoFileName, {flags: 'w', ecnoding: 'utf8', mode: 0666} );
    }
    tree= t;
    events.on('flush', flush_stats);
  }

  // This is rubbish, needs to protect against the inevitable race here.
  ceres.CeresTree.getTree(config.ceres.filePath, function(err, t ) {
    if( err || !t ) { 
      ceres.CeresTree.create(config.ceres.filePath, {meh: false}, function(err, t) {
         if( err ) {
           if( debug ) { l.log( err, "ERROR" ); }
           //urk handle it :(
         }
         else {
           begin( t, "Created ceres tree successfully" );
         }
      });
    } else {
      begin( t, "Loaded existing ceres tree successfully" );
    }
  });

  return true;
};
