###
#
# Calls notifications to subscribed clients
#
###

beanstalkHost             = process.env.BEANSTALK_HOST            or '127.0.0.1'
beanstalkPort             = process.env.BEANSTALK_PORT            or 11300
MAX_RETRIES               = process.env.MAX_RETRIES               or 30
CLIENT_TIMEOUT            = process.env.CLIENT_TIMEOUT            or 10000  # <-- clients must respond in this amount of time
MAX_QUEUE_SIZE            = process.env.MAX_QUEUE_SIZE            or 5
DEBUG                     = !!(process.env.DEBUG                  or false)
NOTIFICATIONS_OUT_TUBE    = process.env.NOTIFICATIONS_OUT_TUBE    or 'notifications_out'
NOTIFICATIONS_RETURN_TUBE = process.env.NOTIFICATIONS_RETURN_TUBE or 'notifications_return'

http        = require('http')
nodestalker = require('nodestalker')
rest        = require('restler')
moment      = require('moment')
figlet      = require('figlet')

# get one global write client


RETRY_PRIORITY = 11
RETRY_DELAY    = 5 # <-- backoff is this times the number of attempts
                   #     total time is 1*5 + 2*5 + 3*5 + ...


MAX_SHUTDOWN_DELAY = CLIENT_TIMEOUT + 1000  # <-- when shutting down, never wait longer than this for a response from any client

# listen
jobCount = 0
reserveJob = ()->
    if jobCount >= MAX_QUEUE_SIZE
        if DEBUG then console.log "[#{new Date().toString()}] jobCount of #{jobCount} has reached maximum.  Delaying."
        setTimeout(reserveJob, 500)
        return

    # if DEBUG then console.log "[#{new Date().toString()}] connecting to beanstalk"
    beanstalkReadClient = nodestalker.Client("#{beanstalkHost}:#{beanstalkPort}")
    beanstalkReadClient.watch(NOTIFICATIONS_OUT_TUBE).onSuccess ()->
        beanstalkReadClient.reserve().onSuccess (job)->
            ++jobCount

            # watch for another job
            reserveJob()

            processJob job, (result)->
                --jobCount
                if DEBUG then console.log "[#{new Date().toString()}] deleting job #{job.id}"
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

    try

        if DEBUG then console.log "[#{new Date().toString()}] begin processJob "+job.id+" (notificationId #{jobData.meta.id}, attempt #{jobData.meta.attempt} of #{MAX_RETRIES}, href #{href})"
        rest.post(href, {
            headers: {'User-Agent': 'XChain Webhooks'}
            timeout: CLIENT_TIMEOUT
            data: JSON.stringify({
                id: jobData.meta.id
                time: moment().utc().format()
                attempt: jobData.meta.attempt
                apiToken: jobData.meta.apiToken if jobData.meta.apiToken?
                signature: jobData.meta.signature if jobData.meta.signature?
                payload: jobData.payload
            })
        }).on 'complete', (data, response)->
            msg = ''
            if response
                if DEBUG then console.log "[#{new Date().toString()}] received HTTP response: "+response?.statusCode?.toString()
            else
                if DEBUG then console.log "[#{new Date().toString()}] received no HTTP response"
            if response? and response.statusCode.toString().charAt(0) == '2'
                success = true
            else
                success = false
                if response?
                    msg = "ERROR: received HTTP response with code "+response.statusCode
                else
                    if data instanceof Error
                        msg = ""+data
                    else
                        msg = "ERROR: no HTTP response received"

            # if DEBUG then console.log "[#{new Date().toString()}] #{job.id} finish success=#{success}"
            finishJob(success, msg, jobData, job, callback)
            return

        .on 'timeout', (e)->
            if DEBUG then console.log "[#{new Date().toString()}] #{job.id} timeout", e
            # finishJob(false, "Timeout: "+e, jobData, job, callback)
            return

        .on 'error', (e)->
            if DEBUG then console.log "[#{new Date().toString()}] #{job.id} http error", e
            # finishJob(false, "Error: "+e, jobData, job, callback)
            return

    catch err
         if DEBUG then console.log "[#{new Date().toString()}] Caught ERROR:",err
         finishJob(false, "Unexpected error: "+err, jobData, job, callback)
         return

    return

finishJob = (success, err, jobData, job, callback)->
    if DEBUG then console.log "[#{new Date().toString()}] end "+job.id+""

    # if done
    #   then push the job back to the beanstalk notification_result queue with the new state
    finished = false
    if success
        finished = true
    else
        # error
        if DEBUG then console.log "[#{new Date().toString()}] error - retrying | #{err}"
        if jobData.meta.attempt >= MAX_RETRIES
            if DEBUG then console.log "[#{new Date().toString()}] giving up after attempt #{jobData.meta.attempt}"
            finished = true


    if finished
        if DEBUG then console.log "[#{new Date().toString()}] inserting final notification status | success=#{success} | #{err}"
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
        insertJobIntoBeanstalk NOTIFICATIONS_RETURN_TUBE, queueEntry, 10, 0, (loadSuccess)->
            if loadSuccess
                callback(true)
            return
    else
        # retry
        insertJobIntoBeanstalk NOTIFICATIONS_OUT_TUBE, jobData, RETRY_PRIORITY, RETRY_DELAY * jobData.meta.attempt, (loadSuccess)->
            if loadSuccess
                callback(true)
            return
    return



# beanstalk
insertJobIntoBeanstalk = (queue, data, retry_priority, retry_delay, callback)->
    beanstalkWriteClient = nodestalker.Client("#{beanstalkHost}:#{beanstalkPort}")
    beanstalkWriteClient.use(queue).onSuccess ()->
        beanstalkWriteClient.put(JSON.stringify(data), retry_priority, retry_delay)
        .onSuccess ()->
            # if DEBUG then console.log "job loaded"
            callback(true)
            return
        .onError ()->
            if DEBUG then console.log "[#{new Date().toString()}] error loading job to #{queue}"
            callback(false)
        return
    .onError ()->
        if DEBUG then console.log "[#{new Date().toString()}] error connecting to beanstalk"
        callback(false)
    return




gracefulShutdown = (callback)->
    startTimestamp = new Date().getTime()
    if DEBUG then console.log "[#{new Date().toString()}] begin shutdown"

    intervalReference = setInterval(()->
        if jobCount == 0 or (new Date().getTime() - startTimestamp >= MAX_SHUTDOWN_DELAY)
            if jobCount > 0
                if DEBUG then console.log "[#{new Date().toString()}] Gave up waiting on #{jobCount} job(s)"
            if DEBUG then console.log "[#{new Date().toString()}] shutdown complete"
            clearInterval(intervalReference)
            callback()
        else
            if DEBUG then console.log "[#{new Date().toString()}] waiting on #{jobCount} job(s)"
    , 250)

# signal handler
process.on "SIGTERM", ->
    if DEBUG then console.log "[#{new Date().toString()}] caught SIGTERM"
    gracefulShutdown ()->
        process.exit 0
        return

process.on "SIGINT", ->
    if DEBUG then console.log "[#{new Date().toString()}] caught SIGINT"
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
, 10


