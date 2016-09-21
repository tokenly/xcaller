###
#
# Calls notifications to subscribed clients
#
###

beanstalkHost             = process.env.BEANSTALK_HOST            or '127.0.0.1'
beanstalkPort             = process.env.BEANSTALK_PORT            or 11300
NOTIFICATIONS_OUT_TUBE    = process.env.NOTIFICATIONS_OUT_TUBE    or 'notifications_out'
COUNT                     = process.env.COUNT                     or 20


fivebeans    = require('fivebeans')

href = "http://127.0.0.1:8099/index.php"
client = new fivebeans.client(beanstalkHost, beanstalkPort)
client.on 'connect', ()->
    client.use NOTIFICATIONS_OUT_TUBE, (err, tubename)->
        for i in [0...COUNT]
            payload = {
                meta: {
                    id: 1000+i
                    attempt: 0
                    endpoint: href
                    returnTubeName: 'notifications_return'
                }
                payload: JSON.stringify({
                    name: "Job #{i+1}"
                    ts: (Date.now())
                    foo: 'bar'
                })
            }
            client.put 10, 0, 300, JSON.stringify(payload), (err, jobid)->
                if err
                    console.log "err: "+err
                console.log "put job #{jobid}"
        client.end()


client.connect()
