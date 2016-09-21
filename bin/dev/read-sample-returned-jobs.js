// Generated by CoffeeScript 1.10.0

/*
 *
 * Calls notifications to subscribed clients
 *
 */

(function() {
  var EventEmitter, NOTIFICATIONS_RETURN_TUBE, beanstalkHost, beanstalkPort, client, events, fivebeans, href, results;

  beanstalkHost = process.env.BEANSTALK_HOST || '127.0.0.1';

  beanstalkPort = process.env.BEANSTALK_PORT || 11300;

  NOTIFICATIONS_RETURN_TUBE = process.env.NOTIFICATIONS_RETURN_TUBE || 'notifications_return';

  fivebeans = require('fivebeans');

  EventEmitter = require('events').EventEmitter;

  events = new EventEmitter();

  results = {
    total: 0,
    success: 0,
    error: 0,
    errors: []
  };

  href = "http://127.0.0.1:8099/index.php";

  client = new fivebeans.client(beanstalkHost, beanstalkPort);

  client.on('connect', function() {
    return client.watch(NOTIFICATIONS_RETURN_TUBE, function(err, numWatching) {
      return events.emit('next');
    });
  });

  events.on('next', function() {
    return client.reserve_with_timeout(1, function(err, jobid, rawPayload) {
      var payload;
      if (err === 'TIMED_OUT') {
        console.log("closing..");
        client.end();
        return;
      }
      payload = JSON.parse(rawPayload);
      ++results.total;
      if (payload.data["return"].success) {
        ++results.success;
      } else {
        ++results.error;
        results.errors.push(payload.data["return"].error);
      }
      return client.destroy(jobid, function(err) {
        return events.emit('next');
      });
    });
  });

  client.on('close', function() {
    console.log("closed");
    return console.log("results: " + JSON.stringify(results, null, 2));
  });

  client.connect();

}).call(this);