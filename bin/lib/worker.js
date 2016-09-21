// Generated by CoffeeScript 1.10.0
(function() {
  var BURY_PRIORITY, DEFAULT_TTR, EventEmitter, RESERVE_WAIT_TIME, RETRY_DELAY, RETRY_PRIORITY, exports, fivebeans, moment, rest, util;

  fivebeans = require('fivebeans');

  rest = require('restler');

  moment = require('moment');

  util = require('util');

  EventEmitter = require('events').EventEmitter;

  RETRY_PRIORITY = 11;

  RETRY_DELAY = 5;

  RESERVE_WAIT_TIME = 2.5;

  BURY_PRIORITY = 1024;

  DEFAULT_TTR = 300;

  exports = {};

  exports.buildWorker = function(opts) {
    var beanstalkHost, beanstalkPort, client, clientClosed, clientConnected, clientErrored, clientTimeout, connected, finishJob, initClients, insertJobIntoBeanstalk, logger, maxRetries, outTube, processJob, ref, ref1, ref2, ref3, ref4, ref5, ref6, ref7, returnTube, runNextJob, self, shutdownClient, shutdownRequested, startingUpPhase, watchTubes, workerId, writeClient, writeClientClosed, writeClientConnected, writeClientErrored, writeClientReady;
    workerId = (function() {
      if ((ref = opts.workerId) != null) {
        return ref;
      } else {
        throw new Error('workerId is required');
      }
    })();
    beanstalkHost = (function() {
      if ((ref1 = opts.beanstalkHost) != null) {
        return ref1;
      } else {
        throw new Error('beanstalkHost is required');
      }
    })();
    beanstalkPort = (function() {
      if ((ref2 = opts.beanstalkPort) != null) {
        return ref2;
      } else {
        throw new Error('beanstalkPort is required');
      }
    })();
    outTube = (function() {
      if ((ref3 = opts.outTube) != null) {
        return ref3;
      } else {
        throw new Error('outTube is required');
      }
    })();
    returnTube = (function() {
      if ((ref4 = opts.returnTube) != null) {
        return ref4;
      } else {
        throw new Error('returnTube is required');
      }
    })();
    clientTimeout = (function() {
      if ((ref5 = opts.clientTimeout) != null) {
        return ref5;
      } else {
        throw new Error('clientTimeout is required');
      }
    })();
    maxRetries = (function() {
      if ((ref6 = opts.maxRetries) != null) {
        return ref6;
      } else {
        throw new Error('maxRetries is required');
      }
    })();
    logger = (function() {
      if ((ref7 = opts.logger) != null) {
        return ref7;
      } else {
        throw new Error('logger is required');
      }
    })();
    self = new EventEmitter();
    clientConnected = function() {
      var connected;
      logger.silly("worker " + workerId + " connected");
      connected = true;
      watchTubes();
    };
    clientErrored = function(err) {
      logger.warn("client error", {
        name: 'client.error',
        error: err,
        workerId: workerId
      });
      self.emit('error.client', err);
      if (!shutdownRequested) {
        shutdownClient();
      }
    };
    clientClosed = function() {
      var connected;
      connected = false;
      self.running = false;
      self.busy = false;
      logger.silly("client closed", {
        workerId: workerId
      });
      self.emit('closed', workerId);
    };
    shutdownClient = function() {
      var connected;
      client.end();
      writeClient.end();
      connected = false;
      self.running = false;
      self.busy = false;
    };
    watchTubes = function() {
      return client.watch(outTube, function(err, numwatched) {
        if (err) {
          logger.warn("error watching outTube", {
            name: 'outTubeWatch.error',
            error: err,
            outTube: outTube,
            workerId: workerId
          });
          shutdownClient();
          return;
        }
        self.emit('next');
      });
    };
    runNextJob = function() {
      if (shutdownRequested) {
        shutdownClient();
        return;
      }
      return client.reserve_with_timeout(RESERVE_WAIT_TIME, function(err, jobid, payload) {
        var e, error, jobData;
        if (err) {
          if (err === 'TIMED_OUT') {
            self.emit('loop');
            self.emit('next');
            return;
          }
          logger.warn("client error", {
            name: 'client.error',
            error: err,
            workerId: workerId
          });
          shutdownClient();
          return;
        }
        try {
          logger.silly("running job " + jobid);
          jobData = JSON.parse(payload);
          console.log("jobData is " + JSON.stringify(jobData));
          self.busy = true;
          processJob(jobid, jobData, function(completed) {
            client.destroy(jobid, function(err) {
              self.busy = false;
              if (err) {
                logger.warn("destroy error", {
                  name: 'error.destroy',
                  error: err,
                  jobid: jobid,
                  workerId: workerId
                });
                shutdownClient();
                return;
              }
              self.emit('next');
            });
          });
        } catch (error) {
          e = error;
          self.busy = false;
          logger.warn("error.uncaught " + e, {
            name: 'error.uncaught',
            error: '' + e,
            jobid: jobid,
            workerId: workerId
          });
          self.emit('error.job', e);
          shutdownClient();
        }
      });
    };
    processJob = function(jobid, jobData, callback) {
      var err, error, href, ref8, ref9, success;
      success = false;
      if (((ref8 = jobData.meta) != null ? ref8.attempt : void 0) == null) {
        logger.warn("error.badJob", {
          name: 'error.badJob',
          jobData: JSON.stringify(jobData),
          jobid: jobid,
          workerId: workerId
        });
        callback(true);
        return;
      }
      try {
        jobData.meta.attempt = jobData.meta.attempt + 1;
        href = jobData.meta.endpoint;
        if (href == null) {
          throw new Error("meta.endpoint was empty");
        }
        logger.info("begin processing job", {
          name: 'job.begin',
          jobId: jobid,
          notificationId: jobData.meta.id,
          attempt: jobData.meta.attempt,
          maxRetries: maxRetries,
          href: href
        });
        rest.post(href, {
          headers: {
            'User-Agent': 'Tokenly XCaller'
          },
          timeout: clientTimeout,
          data: JSON.stringify({
            id: jobData.meta.id,
            time: moment().utc().format(),
            attempt: jobData.meta.attempt,
            apiToken: jobData.meta.apiToken != null ? jobData.meta.apiToken : void 0,
            signature: jobData.meta.signature != null ? jobData.meta.signature : void 0,
            payload: jobData.payload
          })
        }).on('complete', function(data, response) {
          var msg, ref10, ref11, ref12, ref13, ref9;
          msg = '';
          if (response) {
            logger.silly("received HTTP response code " + (response != null ? (ref9 = response.statusCode) != null ? ref9.toString() : void 0 : void 0), {
              name: 'job.response',
              jobId: jobid,
              notificationId: jobData.meta.id,
              response: response != null ? (ref10 = response.statusCode) != null ? ref10.toString() : void 0 : void 0
            });
          } else {
            logger.warn("received no HTTP response", {
              name: 'job.noResponse',
              jobId: jobid,
              notificationId: jobData.meta.id,
              href: href
            });
          }
          if ((response != null) && response.statusCode.toString().charAt(0) === '2') {
            logger.info("received valid HTTP response", {
              name: 'job.validResponse',
              jobId: jobid,
              notificationId: jobData.meta.id,
              status: response != null ? (ref11 = response.statusCode) != null ? ref11.toString() : void 0 : void 0
            });
            success = true;
          } else {
            logger.warn("received invalid HTTP response with response code " + (response != null ? (ref12 = response.statusCode) != null ? ref12.toString() : void 0 : void 0), {
              name: 'job.invalidResponse',
              jobId: jobid,
              notificationId: jobData.meta.id,
              status: response != null ? (ref13 = response.statusCode) != null ? ref13.toString() : void 0 : void 0,
              href: href
            });
            success = false;
            if (response != null) {
              msg = "received HTTP response with code " + response.statusCode;
            } else {
              if (data instanceof Error) {
                msg = "" + data;
              } else {
                msg = "no HTTP response received";
              }
            }
          }
          finishJob(success, msg, jobData, jobid, callback);
        }).on('timeout', function(e) {
          logger.warn("timeout waiting for HTTP response", {
            name: 'job.timeout',
            jobId: jobid,
            notificationId: jobData.meta.id,
            timeout: clientTimeout,
            href: href
          });
          finishJob(false, "Timeout: " + e, jobData, jobid, callback);
        }).on('error', function(e) {
          logger.error("HTTP error", {
            name: 'job.httpError',
            jobId: jobid,
            notificationId: jobData.meta.id,
            error: e.message,
            href: href
          });
        });
      } catch (error) {
        err = error;
        console.error(err);
        logger.error("Unexpected error", {
          name: 'job.errorUnexpected',
          jobId: jobid,
          notificationId: ((ref9 = jobData.meta) != null ? ref9.id : void 0) != null,
          error: err.message,
          href: href
        });
        finishJob(false, "Unexpected error: " + err, jobData, jobid, callback);
        return;
      }
    };
    finishJob = function(success, errString, jobData, jobid, callback) {
      var _logData, finished, queueEntry, ref8, ref9, returnJobName;
      finished = false;
      if (success) {
        finished = true;
      } else {
        if (jobData.meta.attempt >= maxRetries) {
          logger.warn("Job giving up after attempt " + jobData.meta.attempt, {
            name: 'job.failed',
            jobId: jobid,
            notificationId: jobData.meta.id,
            attempts: jobData.meta.attempt
          });
          finished = true;
        } else {
          logger.debug("Retrying job after error", {
            name: 'job.retry',
            jobId: jobid,
            notificationId: jobData.meta.id,
            attempts: jobData.meta.attempt,
            error: errString
          });
        }
      }
      if (finished) {
        _logData = {
          name: 'job.finished',
          jobId: jobid,
          notificationId: jobData.meta.id,
          href: jobData.meta.endpoint,
          totalAttempts: jobData.meta.attempt,
          success: success
        };
        if (errString) {
          _logData.error = errString;
        }
        logger.info("Job finished", _logData);
        returnTube = (ref8 = jobData.meta.returnTubeName) != null ? ref8 : returnTube;
        if (returnTube) {
          returnJobName = (ref9 = jobData.meta.returnJobName) != null ? ref9 : "App\\Jobs\\XChain\\NotificationReturnJob";
          jobData["return"] = {
            success: success,
            error: errString,
            timestamp: new Date().getTime(),
            totalAttempts: jobData.meta.attempt
          };
          queueEntry = {
            job: returnJobName,
            data: jobData
          };
          insertJobIntoBeanstalk(returnTube, queueEntry, 10, 0, function(loadSuccess) {
            callback(loadSuccess);
          });
        } else {
          callback(true);
        }
      } else {
        insertJobIntoBeanstalk(outTube, jobData, RETRY_PRIORITY, RETRY_DELAY * jobData.meta.attempt, function(loadSuccess) {
          return callback(loadSuccess);
        });
      }
    };
    writeClientConnected = function() {
      var writeClientReady;
      writeClientReady = true;
    };
    writeClientErrored = function(err) {
      logger.warn("writeClient error", {
        name: 'error.writeClient',
        error: err,
        workerId: workerId
      });
      shutdownClient();
    };
    writeClientClosed = function() {
      var writeClientReady;
      writeClientReady = false;
      logger.silly("writeClient closed", {
        workerId: workerId
      });
      if (startingUpPhase) {
        self.emit('closed', workerId);
      }
    };
    insertJobIntoBeanstalk = function(queue, data, retry_priority, retry_delay, callback, attempts) {
      if (attempts == null) {
        attempts = 0;
      }
      writeClient.use(queue, function(err, tubename) {
        if (err) {
          logger.warn("error selecting queue", {
            name: 'error.writeClientWatch',
            error: err,
            queue: queue,
            workerId: workerId
          });
          callback(false);
          return;
        }
        if (tubename !== queue) {
          ++attempts;
          if (attempts > 3) {
            logger.warn("queue mismatch", {
              name: 'error.fatalQueueMismatch',
              attempts: attempts,
              tubename: tubename,
              queue: queue,
              workerId: workerId
            });
          } else {
            logger.warn("queue mismatch", {
              name: 'error.queueMismatch',
              attempts: attempts,
              tubename: tubename,
              queue: queue,
              workerId: workerId
            });
            setTimeout(function() {
              return insertJobIntoBeanstalk(queue, data, retry_priority, retry_delay, callback, attempts);
            }, attempts * 125);
          }
          return;
        }
        return writeClient.put(retry_priority, retry_delay, DEFAULT_TTR, JSON.stringify(data), function(err, jobid) {
          if (err) {
            logger.warn("error loading job", {
              name: 'loadJob.failed',
              queue: queue
            });
            callback(false);
            return;
          }
          callback(true);
        });
      });
    };
    client = null;
    writeClient = null;
    writeClientReady = false;
    connected = false;
    shutdownRequested = false;
    self.running = false;
    self.busy = false;
    startingUpPhase = false;
    self.on('next', runNextJob);
    initClients = function() {
      client = new fivebeans.client(beanstalkHost, beanstalkPort);
      client.on('connect', clientConnected);
      client.on('error', clientErrored);
      client.on('close', clientClosed);
      writeClient = new fivebeans.client(beanstalkHost, beanstalkPort);
      writeClientReady = false;
      writeClient.on('connect', writeClientConnected);
      writeClient.on('connect', function() {
        startingUpPhase = false;
        client.connect();
      });
      writeClient.on('error', writeClientErrored);
      writeClient.on('close', writeClientClosed);
    };
    self.getId = function() {
      return workerId;
    };
    self.run = function() {
      logger.silly("launching worker " + workerId);
      client = null;
      writeClient = null;
      shutdownRequested = false;
      self.running = true;
      startingUpPhase = true;
      initClients();
      writeClient.connect();
    };
    self.stop = function() {
      shutdownRequested = true;
    };
    return self;
  };

  module.exports = exports;

}).call(this);
