###
#
# Calls notifications to subscribed clients
#
###

beanstalkHost             = process.env.BEANSTALK_HOST            or '127.0.0.1'
beanstalkPort             = process.env.BEANSTALK_PORT            or 11300
NOTIFICATIONS_RETURN_TUBE = process.env.NOTIFICATIONS_RETURN_TUBE or 'notifications_return'


fivebeans    = require('fivebeans')
EventEmitter = require('events').EventEmitter
events = new EventEmitter()

results = {total:0, success: 0, error: 0, errors: []}

href = "http://127.0.0.1:8099/index.php"
client = new fivebeans.client(beanstalkHost, beanstalkPort)
client.on 'connect', ()->
    client.watch NOTIFICATIONS_RETURN_TUBE, (err, numWatching)->
        events.emit('next')

events.on 'next', ()->
    client.reserve_with_timeout 1, (err, jobid, rawPayload)->
        # console.log "err", err
        if err == 'TIMED_OUT'
            console.log "closing.."
            client.end()
            return

        payload = JSON.parse(rawPayload)

        ++results.total
        if payload.data.return.success
            ++results.success
        else
            ++results.error
            results.errors.push(payload.data.return.error)

        client.destroy jobid, (err)->
            events.emit('next')


client.on 'close', ()->
    console.log "closed"
    console.log "results: "+JSON.stringify(results, null, 2)

client.connect()

