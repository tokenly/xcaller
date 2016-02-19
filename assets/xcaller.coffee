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
JOB_LOG_FILENAME          = process.env.JOB_LOG_FILENAME          or null
VERSION                   = '0.2.1'

http        = require('http')
nodestalker = require('nodestalker')
rest        = require('restler')
moment      = require('moment')
figlet      = require('figlet')
winston     = require('winston')


RETRY_PRIORITY = 11
RETRY_DELAY    = 5 # <-- backoff is this times the number of attempts
                   #     total time is 1*5 + 2*5 + 3*5 + ...


MAX_SHUTDOWN_DELAY = CLIENT_TIMEOUT + 1000  # <-- when shutting down, never wait longer than this for a response from any client


# init winston logging
logger = new winston.Logger({
    transports: []
    exitOnError: false
})
logger.add(winston.transports.Console, {
    handleExceptions: true
    timestamp: ()->
        return new Date().toString()
    formatter: (options)->
        return "[#{options.timestamp()}] #{options.level.toUpperCase()} "+
            "#{if undefined != options.message then options.message+' ' else ''}"+
            (if options.meta && Object.keys(options.meta).length then JSON.stringify(options.meta) else '' )
    level: if DEBUG then 'debug' else 'info'
})
if JOB_LOG_FILENAME
    translateLevel = (levelString)->
        map = {
            silly:     50
            debug:     100
            info:      200
            notice:    250
            warning:   300
            error:     400
            critical:  500
            alert:     550
            emergency: 600
        }
        if map[levelString]? then return map[levelString]
        return 0

    logger.add(winston.transports.File, {
        filename: JOB_LOG_FILENAME,
        level: 'debug',
        json: false,
        timestamp: ()->
            return new Date().toISOString()
        formatter: (options)->
            jsonData = JSON.parse(JSON.stringify(options.meta))
            jsonData.level     = translateLevel(options.level)
            jsonData.timestamp = options.timestamp()
            jsonData.message   = options.message
            return JSON.stringify(jsonData)
    })


# listen
jobCount = 0
reserveJob = ()->
    if jobCount >= MAX_QUEUE_SIZE
        logger.info("jobCount of #{jobCount} has reached maximum.  Delaying.", {name: 'jobCount.exceeded', count: jobCount, maximum: MAX_QUEUE_SIZE, })
        setTimeout(reserveJob, 500)
        return

    beanstalkReadClient = nodestalker.Client("#{beanstalkHost}:#{beanstalkPort}")
    beanstalkReadClient.watch(NOTIFICATIONS_OUT_TUBE).onSuccess ()->
        beanstalkReadClient.reserve().onSuccess (job)->
            ++jobCount

            # watch for another job
            reserveJob()

            processJob job, (result)->
                --jobCount
                logger.silly("deleting job", {name: 'job.delete', jobId: job.id, })
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

        logger.info("begin processing job", {name: 'job.begin', jobId: job.id, notificationId: jobData.meta.id, attempt: jobData.meta.attempt, maxRetries: MAX_RETRIES, href: href, })
        rest.post(href, {
            headers: {'User-Agent': 'XCaller'}
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
                logger.silly("received HTTP response code #{response?.statusCode?.toString()}", {name: 'job.response', jobId: job.id, notificationId: jobData.meta.id, response: response?.statusCode?.toString(), })
            else
                logger.warn("received no HTTP response", {name: 'job.noResponse', jobId: job.id, notificationId: jobData.meta.id, href: href, })

            if response? and response.statusCode.toString().charAt(0) == '2'
                logger.info("received valid HTTP response", {name: 'job.validResponse', jobId: job.id, notificationId: jobData.meta.id, status: response?.statusCode?.toString(), })
                success = true
            else
                logger.warn("received invalid HTTP response with response code #{response?.statusCode?.toString()}", {name: 'job.invalidResponse', jobId: job.id, notificationId: jobData.meta.id, status: response?.statusCode?.toString(), href: href, })

                success = false
                if response?
                    msg = "ERROR: received HTTP response with code "+response.statusCode
                else
                    if data instanceof Error
                        msg = ""+data
                    else
                        msg = "ERROR: no HTTP response received"

            finishJob(success, msg, jobData, job, callback)
            return

        .on 'timeout', (e)->
            logger.warn("timeout waiting for HTTP response", {name: 'job.timeout', jobId: job.id, notificationId: jobData.meta.id, timeout: CLIENT_TIMEOUT, href: href, })

            # 'complete' will not be called on a timeout failure
            finishJob(false, "Timeout: "+e, jobData, job, callback)
            
            return

        .on 'error', (e)->
            # 'complete' will be called after this
            logger.error("HTTP error", {name: 'job.httpError', jobId: job.id, notificationId: jobData.meta.id, error: e, href: href, })
            return

    catch err
        logger.error("Unexpected error", {name: 'job.error', jobId: job.id, notificationId: jobData.meta.id, error: err, href: href, })
        finishJob(false, "Unexpected error: "+err, jobData, job, callback)
        return

    return

finishJob = (success, err, jobData, job, callback)->
    logger.silly("Job finished", {name: 'job.finish', jobId: job.id, notificationId: jobData.meta.id, })

    # if done
    #   then push the job back to the beanstalk notification_result queue with the new state
    finished = false
    if success
        finished = true
    else
        # error
        if jobData.meta.attempt >= MAX_RETRIES
            logger.warn("Job giving up after attempt #{jobData.meta.attempt}", {name: 'job.failed', jobId: job.id, notificationId: jobData.meta.id, attempts: jobData.meta.attempt, })
            finished = true
        else
            logger.debug("Retrying job after error", {name: 'job.retry', jobId: job.id, notificationId: jobData.meta.id, attempts: jobData.meta.attempt, error: err, })



    if finished
        logger.info("Job finished", {name: 'job.finished', jobId: job.id, notificationId: jobData.meta.id, href: jobData.meta.endpoint, totalAttempts: jobData.meta.attempt, success: success, error: err, })

        returnTube = jobData.meta.returnTubeName ? NOTIFICATIONS_RETURN_TUBE
        if returnTube
            returnJobName = jobData.meta.returnJobName ? "App\\Jobs\\XChain\\NotificationReturnJob"
            jobData.return = {
                success: success
                error: err
                timestamp: new Date().getTime()
                totalAttempts: jobData.meta.attempt
            }
            queueEntry = {
                job: returnJobName
                data: jobData
            }
            insertJobIntoBeanstalk returnTube, queueEntry, 10, 0, (loadSuccess)->
                if loadSuccess
                    callback(true)
                return
        else
            # no return tube defined - just do the callback to finish the job
            callback(true)
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
            callback(true)
            return
        .onError ()->
            logger.error("Error loading job", {name: 'beanstalkLoad.failed', queue: queue, })
            callback(false)
        return
    .onError ()->
        logger.error("error connecting to beanstalk", {name: 'beanstalkConnect.error', beanstalkHost: beanstalkHost, beanstalkPort: beanstalkPort, })
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
        logger.debug('end server', {name: 'lifecycle.stop', signal: 'SiGTERM', })
        process.exit 0
        return

process.on "SIGINT", ->
    if DEBUG then console.log "[#{new Date().toString()}] caught SIGINT"
    gracefulShutdown ()->
        logger.debug('end server', {name: 'lifecycle.stop', signal: 'SIGINT', })
        process.exit 0
        return
    return


figlet.text "Tokenly XCaller", 'Slant', (err, data)->
    process.stdout.write "#{data}\n\nVersion #{VERSION}\nconnecting to beanstalkd at #{beanstalkHost}:#{beanstalkPort}\n\n"
    return

# run the reserver
setTimeout ()->
    logger.debug('start server', {name: 'lifecycle.start', })
    reserveJob()
, 10


