###
#
# This simple server loads events into a beanstalkd queue
#
###

beanstalkHost = process.env.BEANSTALK_HOST or '127.0.0.1'
beanstalkPort = process.env.BEANSTALK_PORT or 11300

http = require('http')

nodestalker = require('nodestalker')
rest = require('restler')
moment = require('moment')
figlet = require('figlet');

# get one global write client
beanstalkWriteClient = nodestalker.Client("#{beanstalkHost}:#{beanstalkPort}")


RETRY_PRIORITY = 11
RETRY_DELAY    = 5 # <-- backoff is this times the number of attempts
MAX_RETRIES    = 3
# total time is 1*5 + 2*5 + 3*5 = 30 seconds


CLIENT_TIMEOUT     = 4000  # <-- clients must respond in this amount of time
MAX_SHUTDOWN_DELAY = 5000  # <-- when shutting down, never wait longer than this for a response from any client

# listen
jobCount = 0
reserveJob = ()->
    console.log "[#{new Date().toString()}] connecting to beanstalk"
    beanstalkReadClient = nodestalker.Client("#{beanstalkHost}:#{beanstalkPort}")
    beanstalkReadClient.watch('notifications_out').onSuccess ()->
        beanstalkReadClient.reserve().onSuccess (job)->
            ++jobCount

            # watch for another job
            reserveJob()

            processJob job, (result)->
                --jobCount
                console.log "[#{new Date().toString()}] deleting job #{job.id}"
                beanstalkReadClient.deleteJob(job.id).onSuccess (del_msg)->
                    # deleted
                    #   end this connection
                    beanstalkReadClient.disconnect()
                    return

                # job processed
                return
            return
        return
    return


processJob = (job, callback)->
    jobData = JSON.parse(job.data)

    success = false

    # call the callback
    jobData.meta.attempt = jobData.meta.attempt + 1
    href = jobData.meta.endpoint

    console.log "[#{new Date().toString()}] begin processJob "+job.id+" (notificationId #{jobData.meta.id}, attempt #{jobData.meta.attempt} of #{MAX_RETRIES}, href #{href})"
    rest.post(href, {
        headers: {'User-Agent': 'XChain Webhooks'}
        timeout: CLIENT_TIMEOUT
        data: JSON.stringify({
            id: jobData.meta.id
            time: moment().utc().format()
            attempt: jobData.meta.attempt
            apiToken: jobData.meta.apiToken
            signature: jobData.meta.signature
            payload: jobData.payload
        })
    }).on 'complete', (data, response)->
        msg = ''
        console.log "[#{new Date().toString()}] received HTTP response: "+response.statusCode.toString()
        if response.statusCode.toString().charAt(0) == '2'
            success = true
        else
            success = false
            msg = "ERROR: received HTTP response with code "+response.statusCode
        finishJob(success, msg)
        return

    .on 'timeout', (e)->
        console.log "[#{new Date().toString()}] timeout",e
        finishJob(false, "Timeout: "+e)
        return

    .on 'error', (e)->
        console.log "[#{new Date().toString()}] error",e
        finishJob(false, "Error: "+e)
        return

    finishJob = (success, err)->
        console.log "[#{new Date().toString()}] end "+job.id+""

        # if done
        #   then push the job back to the beanstalk notification_result queue with the new state
        finished = false
        if success
            finished = true
        else
            # error
            console.log "[#{new Date().toString()}] error - retrying"
            if jobData.meta.attempt >= MAX_RETRIES
                console.log "[#{new Date().toString()}] giving up after attempt #{jobData.meta.attempt}"
                finished = true


        if finished
            console.log "[#{new Date().toString()}] inserting final notification status"
            jobData.return = {
                success: success
                error: err
                timestamp: new Date().getTime()
                totalAttempts: jobData.meta.attempt
            }
            queueEntry = {
                job: "App\\Jobs\\XChain\\NotificationReturnJob"
                data: jobData
            }
            insertJobIntoBeanstalk 'notifications_return', queueEntry, 10, 0, (loadSuccess)->
                if loadSuccess
                    callback(true)
                return
        else
            # retry
            insertJobIntoBeanstalk 'notifications_out', jobData, RETRY_PRIORITY, RETRY_DELAY * jobData.meta.attempt, (loadSuccess)->
                if loadSuccess
                    callback(true)
                return
        return

    return


# beanstalk
insertJobIntoBeanstalk = (queue, data, retry_priority, retry_delay, callback)->
    beanstalkWriteClient.use(queue).onSuccess ()->
        beanstalkWriteClient.put(JSON.stringify(data), retry_priority, retry_delay)
        .onSuccess ()->
            # console.log "job loaded"
            callback(true)
            return
        .onError ()->
            console.log "[#{new Date().toString()}] error loading job to #{queue}"
            callback(false)
        return
    .onError ()->
        console.log "[#{new Date().toString()}] error connecting to beanstalk"
        callback(false)
    return




gracefulShutdown = (callback)->
    startTimestamp = new Date().getTime()
    console.log "[#{new Date().toString()}] begin shutdown"

    intervalReference = setInterval(()->
        if jobCount == 0 or (new Date().getTime() - startTimestamp >= MAX_SHUTDOWN_DELAY)
            if jobCount > 0
                console.log "[#{new Date().toString()}] Gave up waiting on #{jobCount} job(s)"
            console.log "[#{new Date().toString()}] shutdown complete"
            clearInterval(intervalReference)
            callback()
        else
            console.log "[#{new Date().toString()}] waiting on #{jobCount} job(s)"
    , 250)

# signal handler
process.on "SIGTERM", ->
    console.log "[#{new Date().toString()}] caught SIGTERM"
    gracefulShutdown ()->
        process.exit 0
        return

process.on "SIGINT", ->
    console.log "[#{new Date().toString()}] caught SIGINT"
    gracefulShutdown ()->
        process.exit 0
        return
    return


figlet.text('Tokenly XCaller', 'Slant', (err, data)->
    process.stdout.write data
    process.stdout.write "\n\n"
    process.stdout.write "connecting to beanstalkd at "+beanstalkHost+":"+beanstalkPort+"\n\n"

    return
)

# run the reserver
setTimeout ()->
    reserveJob()
, 1


