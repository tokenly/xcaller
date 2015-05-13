// Generated by CoffeeScript 1.9.2

/*
#
 * Calls notifications to subscribed clients
#
 */

(function() {
  var CLIENT_TIMEOUT, DEBUG, MAX_QUEUE_SIZE, MAX_RETRIES, MAX_SHUTDOWN_DELAY, NOTIFICATIONS_OUT_TUBE, NOTIFICATIONS_RETURN_TUBE, RETRY_DELAY, RETRY_PRIORITY, beanstalkHost, beanstalkPort, figlet, gracefulShutdown, http, insertJobIntoBeanstalk, jobCount, moment, nodestalker, processJob, reserveJob, rest;

  beanstalkHost = process.env.BEANSTALK_HOST || '127.0.0.1';

  beanstalkPort = process.env.BEANSTALK_PORT || 11300;

  MAX_RETRIES = process.env.MAX_RETRIES || 30;

  CLIENT_TIMEOUT = process.env.CLIENT_TIMEOUT || 10000;

  MAX_QUEUE_SIZE = process.env.MAX_QUEUE_SIZE || 5;

  DEBUG = !!(process.env.DEBUG || false);

  NOTIFICATIONS_OUT_TUBE = process.env.NOTIFICATIONS_OUT_TUBE || 'notifications_out';

  NOTIFICATIONS_RETURN_TUBE = process.env.NOTIFICATIONS_RETURN_TUBE || 'notifications_return';

  http = require('http');

  nodestalker = require('nodestalker');

  rest = require('restler');

  moment = require('moment');

  figlet = require('figlet');

  RETRY_PRIORITY = 11;

  RETRY_DELAY = 5;

  MAX_SHUTDOWN_DELAY = CLIENT_TIMEOUT + 1000;

  jobCount = 0;

  reserveJob = function() {
    var beanstalkReadClient;
    if (jobCount >= MAX_QUEUE_SIZE) {
      if (DEBUG) {
        console.log("[" + (new Date().toString()) + "] jobCount of " + jobCount + " has reached maximum.  Delaying.");
      }
      setTimeout(reserveJob, 500);
      return;
    }
    beanstalkReadClient = nodestalker.Client(beanstalkHost + ":" + beanstalkPort);
    beanstalkReadClient.watch(NOTIFICATIONS_OUT_TUBE).onSuccess(function() {
      beanstalkReadClient.reserve().onSuccess(function(job) {
        ++jobCount;
        reserveJob();
        processJob(job, function(result) {
          --jobCount;
          if (DEBUG) {
            console.log("[" + (new Date().toString()) + "] deleting job " + job.id);
          }
          beanstalkReadClient.deleteJob(job.id).onSuccess(function(del_msg) {
            beanstalkReadClient.disconnect();
          });
        });
      });
    });
  };

  processJob = function(job, callback) {
    var finishJob, href, jobData, success;
    jobData = JSON.parse(job.data);
    success = false;
    jobData.meta.attempt = jobData.meta.attempt + 1;
    href = jobData.meta.endpoint;
    if (DEBUG) {
      console.log(("[" + (new Date().toString()) + "] begin processJob ") + job.id + (" (notificationId " + jobData.meta.id + ", attempt " + jobData.meta.attempt + " of " + MAX_RETRIES + ", href " + href + ")"));
    }
    rest.post(href, {
      headers: {
        'User-Agent': 'XChain Webhooks'
      },
      timeout: CLIENT_TIMEOUT,
      data: JSON.stringify({
        id: jobData.meta.id,
        time: moment().utc().format(),
        attempt: jobData.meta.attempt,
        apiToken: jobData.meta.apiToken != null ? jobData.meta.apiToken : void 0,
        signature: jobData.meta.signature != null ? jobData.meta.signature : void 0,
        payload: jobData.payload
      })
    }).on('complete', function(data, response) {
      var msg, ref;
      msg = '';
      if (response) {
        if (DEBUG) {
          console.log(("[" + (new Date().toString()) + "] received HTTP response: ") + (response != null ? (ref = response.statusCode) != null ? ref.toString() : void 0 : void 0));
        }
      } else {
        if (DEBUG) {
          console.log("[" + (new Date().toString()) + "] received no HTTP response");
        }
      }
      if ((response != null) && response.statusCode.toString().charAt(0) === '2') {
        success = true;
      } else {
        success = false;
        if (response != null) {
          msg = "ERROR: received HTTP response with code " + response.statusCode;
        } else {
          if (data instanceof Error) {
            msg = "" + data;
          } else {
            msg = "ERROR: no HTTP response received";
          }
        }
      }
      finishJob(success, msg);
    }).on('timeout', function(e) {
      if (DEBUG) {
        console.log("[" + (new Date().toString()) + "] " + job.id + " timeout", e);
      }
    }).on('error', function(e) {
      if (DEBUG) {
        console.log("[" + (new Date().toString()) + "] " + job.id + " http error", e);
      }
    });
    finishJob = function(success, err) {
      var finished, queueEntry;
      if (DEBUG) {
        console.log(("[" + (new Date().toString()) + "] end ") + job.id + "");
      }
      finished = false;
      if (success) {
        finished = true;
      } else {
        if (DEBUG) {
          console.log("[" + (new Date().toString()) + "] error - retrying | " + err);
        }
        if (jobData.meta.attempt >= MAX_RETRIES) {
          if (DEBUG) {
            console.log("[" + (new Date().toString()) + "] giving up after attempt " + jobData.meta.attempt);
          }
          finished = true;
        }
      }
      if (finished) {
        if (DEBUG) {
          console.log("[" + (new Date().toString()) + "] inserting final notification status | success=" + success + " | " + err);
        }
        jobData["return"] = {
          success: success,
          error: err,
          timestamp: new Date().getTime(),
          totalAttempts: jobData.meta.attempt
        };
        queueEntry = {
          job: "App\\Jobs\\XChain\\NotificationReturnJob",
          data: jobData
        };
        insertJobIntoBeanstalk(NOTIFICATIONS_RETURN_TUBE, queueEntry, 10, 0, function(loadSuccess) {
          if (loadSuccess) {
            callback(true);
          }
        });
      } else {
        insertJobIntoBeanstalk(NOTIFICATIONS_OUT_TUBE, jobData, RETRY_PRIORITY, RETRY_DELAY * jobData.meta.attempt, function(loadSuccess) {
          if (loadSuccess) {
            callback(true);
          }
        });
      }
    };
  };

  insertJobIntoBeanstalk = function(queue, data, retry_priority, retry_delay, callback) {
    var beanstalkWriteClient;
    beanstalkWriteClient = nodestalker.Client(beanstalkHost + ":" + beanstalkPort);
    beanstalkWriteClient.use(queue).onSuccess(function() {
      beanstalkWriteClient.put(JSON.stringify(data), retry_priority, retry_delay).onSuccess(function() {
        callback(true);
      }).onError(function() {
        if (DEBUG) {
          console.log("[" + (new Date().toString()) + "] error loading job to " + queue);
        }
        return callback(false);
      });
    }).onError(function() {
      if (DEBUG) {
        console.log("[" + (new Date().toString()) + "] error connecting to beanstalk");
      }
      return callback(false);
    });
  };

  gracefulShutdown = function(callback) {
    var intervalReference, startTimestamp;
    startTimestamp = new Date().getTime();
    if (DEBUG) {
      console.log("[" + (new Date().toString()) + "] begin shutdown");
    }
    return intervalReference = setInterval(function() {
      if (jobCount === 0 || (new Date().getTime() - startTimestamp >= MAX_SHUTDOWN_DELAY)) {
        if (jobCount > 0) {
          if (DEBUG) {
            console.log("[" + (new Date().toString()) + "] Gave up waiting on " + jobCount + " job(s)");
          }
        }
        if (DEBUG) {
          console.log("[" + (new Date().toString()) + "] shutdown complete");
        }
        clearInterval(intervalReference);
        return callback();
      } else {
        if (DEBUG) {
          return console.log("[" + (new Date().toString()) + "] waiting on " + jobCount + " job(s)");
        }
      }
    }, 250);
  };

  process.on("SIGTERM", function() {
    if (DEBUG) {
      console.log("[" + (new Date().toString()) + "] caught SIGTERM");
    }
    return gracefulShutdown(function() {
      process.exit(0);
    });
  });

  process.on("SIGINT", function() {
    if (DEBUG) {
      console.log("[" + (new Date().toString()) + "] caught SIGINT");
    }
    gracefulShutdown(function() {
      process.exit(0);
    });
  });

  figlet.text('Tokenly XCaller', 'Slant', function(err, data) {
    process.stdout.write(data);
    process.stdout.write("\n\n");
    process.stdout.write("connecting to beanstalkd at " + beanstalkHost + ":" + beanstalkPort + "\n\n");
  });

  setTimeout(function() {
    return reserveJob();
  }, 10);

}).call(this);
