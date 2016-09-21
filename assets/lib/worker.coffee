fivebeans    = require('fivebeans')
rest         = require('restler')
moment       = require('moment')
util         = require('util')
EventEmitter = require('events').EventEmitter

RETRY_PRIORITY = 11
RETRY_DELAY    = 5 # <-- backoff is this times the number of attempts
                   #     total time is 1*5 + 2*5 + 3*5 + ...


RESERVE_WAIT_TIME = 2.5
BURY_PRIORITY = 1024
DEFAULT_TTR = 300

exports = {}

exports.buildWorker = (opts)->
    workerId      = opts.workerId      ? throw new Error('workerId is required')
    beanstalkHost = opts.beanstalkHost ? throw new Error('beanstalkHost is required')
    beanstalkPort = opts.beanstalkPort ? throw new Error('beanstalkPort is required')
    outTube       = opts.outTube       ? throw new Error('outTube is required')
    returnTube    = opts.returnTube    ? throw new Error('returnTube is required')
    clientTimeout = opts.clientTimeout ? throw new Error('clientTimeout is required')
    maxRetries    = opts.maxRetries    ? throw new Error('maxRetries is required')
    logger        = opts.logger        ? throw new Error('logger is required')

    self = new EventEmitter()

    clientConnected = ()->
        logger.silly("worker #{workerId} connected")
        connected = true
        watchTubes()
        return

    clientErrored = (err)->
        logger.warn("client error", {name: 'client.error', error: err, workerId: workerId})
        self.emit('error.client', err)
        if not shutdownRequested
            shutdownClient()
        return

    clientClosed = ()->
        connected = false
        self.running = false
        self.busy = false
        logger.silly("client closed", {workerId: workerId})
        self.emit('closed', workerId)
        return

    shutdownClient = ()->
        client.end()
        writeClient.end()
        connected = false
        self.running = false
        self.busy = false
        return

    watchTubes = ()->
        client.watch outTube, (err, numwatched)->
            if err
                logger.warn("error watching outTube", {name: 'outTubeWatch.error', error: err, outTube: outTube, workerId: workerId})
                shutdownClient()
                return
            self.emit('next')
            return

    runNextJob = ()->
        if shutdownRequested
            shutdownClient()
            return

        # logger.silly("runNextJob", {workerId: workerId})
        client.reserve_with_timeout RESERVE_WAIT_TIME, (err, jobid, payload)->
            if err
                if err == 'TIMED_OUT'
                    self.emit('loop')
                    self.emit('next')
                    return

                logger.warn("client error", {name: 'client.error', error: err, workerId: workerId})
                shutdownClient()
                return
            try

                logger.silly("running job #{jobid}")
                jobData = JSON.parse(payload)
                console.log "jobData is "+JSON.stringify(jobData)
                self.busy = true
                processJob jobid, jobData, (completed)->
                    client.destroy jobid, (err)->
                        self.busy = false
                        if err
                            logger.warn("destroy error", {name: 'error.destroy', error: err, jobid: jobid, workerId: workerId})
                            shutdownClient()
                            return

                        self.emit('next')
                        return
                    return



            catch e
                self.busy = false
                logger.warn("error.uncaught "+e, {name: 'error.uncaught', error: ''+e, jobid: jobid, workerId: workerId})
                self.emit('error.job', e)
                shutdownClient()
            return



    processJob = (jobid, jobData, callback)->
        success = false

        # handle bad job data
        if not jobData.meta?.attempt?
            logger.warn("error.badJob", {name: 'error.badJob', jobData: JSON.stringify(jobData), jobid: jobid, workerId: workerId})
            callback(true)
            return


        try
            # call the callback
            jobData.meta.attempt = jobData.meta.attempt + 1
            href = jobData.meta.endpoint
            if not href?
                throw new Error("meta.endpoint was empty")

            logger.info("begin processing job", {name: 'job.begin', jobId: jobid, notificationId: jobData.meta.id, attempt: jobData.meta.attempt, maxRetries: maxRetries, href: href, })
            rest.post(href, {
                headers: {'User-Agent': 'Tokenly XCaller'}
                timeout: clientTimeout
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
                    logger.silly("received HTTP response code #{response?.statusCode?.toString()}", {name: 'job.response', jobId: jobid, notificationId: jobData.meta.id, response: response?.statusCode?.toString(), })
                else
                    logger.warn("received no HTTP response", {name: 'job.noResponse', jobId: jobid, notificationId: jobData.meta.id, href: href, })

                if response? and response.statusCode.toString().charAt(0) == '2'
                    logger.info("received valid HTTP response", {name: 'job.validResponse', jobId: jobid, notificationId: jobData.meta.id, status: response?.statusCode?.toString(), })
                    success = true
                else
                    logger.warn("received invalid HTTP response with response code #{response?.statusCode?.toString()}", {name: 'job.invalidResponse', jobId: jobid, notificationId: jobData.meta.id, status: response?.statusCode?.toString(), href: href, })

                    success = false
                    if response?
                        msg = "received HTTP response with code "+response.statusCode
                    else
                        if data instanceof Error
                            msg = ""+data
                        else
                            msg = "no HTTP response received"

                finishJob(success, msg, jobData, jobid, callback)
                return

            .on 'timeout', (e)->
                logger.warn("timeout waiting for HTTP response", {name: 'job.timeout', jobId: jobid, notificationId: jobData.meta.id, timeout: clientTimeout, href: href, })

                # 'complete' will not be called on a timeout failure
                finishJob(false, "Timeout: "+e, jobData, jobid, callback)
                
                return

            .on 'error', (e)->
                # 'complete' will be called after this
                logger.error("HTTP error", {name: 'job.httpError', jobId: jobid, notificationId: jobData.meta.id, error: e.message, href: href, })
                return

        catch err
            console.error(err)
            logger.error("Unexpected error", {name: 'job.errorUnexpected', jobId: jobid, notificationId: jobData.meta?.id?, error: err.message, href: href, })
            finishJob(false, "Unexpected error: "+err, jobData, jobid, callback)
            return

        return

    finishJob = (success, errString, jobData, jobid, callback)->
        # if done
        #   then push the job back to the beanstalk notification_result queue with the new state
        finished = false
        if success
            finished = true
        else
            # error
            if jobData.meta.attempt >= maxRetries
                logger.warn("Job giving up after attempt #{jobData.meta.attempt}", {name: 'job.failed', jobId: jobid, notificationId: jobData.meta.id, attempts: jobData.meta.attempt, })
                finished = true
            else
                logger.debug("Retrying job after error", {name: 'job.retry', jobId: jobid, notificationId: jobData.meta.id, attempts: jobData.meta.attempt, error: errString, })


        if finished
            _logData = {name: 'job.finished', jobId: jobid, notificationId: jobData.meta.id, href: jobData.meta.endpoint, totalAttempts: jobData.meta.attempt, success: success, }
            _logData.error = errString if errString
            logger.info("Job finished", _logData)

            returnTube = jobData.meta.returnTubeName ? returnTube
            if returnTube
                returnJobName = jobData.meta.returnJobName ? "App\\Jobs\\XChain\\NotificationReturnJob"
                jobData.return = {
                    success: success
                    error: errString
                    timestamp: new Date().getTime()
                    totalAttempts: jobData.meta.attempt
                }
                queueEntry = {
                    job: returnJobName
                    data: jobData
                }
                insertJobIntoBeanstalk returnTube, queueEntry, 10, 0, (loadSuccess)->
                    callback(loadSuccess)
                    return
            else
                # no return tube defined - just do the callback to finish the job
                callback(true)
        else
            # retry
            insertJobIntoBeanstalk outTube, jobData, RETRY_PRIORITY, RETRY_DELAY * jobData.meta.attempt, (loadSuccess)->
                callback(loadSuccess)
        return


    # ------------------------------------------------------------------------
    # beanstalk write client

    writeClientConnected = ()->
        writeClientReady = true
        return

    writeClientErrored = (err)->
        logger.warn("writeClient error", {name: 'error.writeClient', error: err, workerId: workerId})
        shutdownClient()
        return

    writeClientClosed = ()->
        writeClientReady = false
        logger.silly("writeClient closed", {workerId: workerId})
        if startingUpPhase
            # since we are still starting up, the main client won't send a closed event
            #   so we have to
            self.emit('closed', workerId)
        return

    insertJobIntoBeanstalk = (queue, data, retry_priority, retry_delay, callback, attempts)->
        if not attempts?
            attempts = 0

        writeClient.use queue, (err, tubename)->
            if err
                logger.warn("error selecting queue", {name: 'error.writeClientWatch', error: err, queue: queue, workerId: workerId})
                callback(false)
                return

            if tubename != queue
                ++attempts
                if attempts > 3
                    logger.warn("queue mismatch", {name: 'error.fatalQueueMismatch', attempts: attempts, tubename: tubename, queue: queue, workerId: workerId})
                else
                    logger.warn("queue mismatch", {name: 'error.queueMismatch', attempts: attempts, tubename: tubename, queue: queue, workerId: workerId})
                    # retry
                    setTimeout ()->
                        insertJobIntoBeanstalk(queue, data, retry_priority, retry_delay, callback, attempts)
                    , attempts * 125
                return




            writeClient.put retry_priority, retry_delay, DEFAULT_TTR, JSON.stringify(data), (err, jobid)->
                if err
                    logger.warn("error loading job", {name: 'loadJob.failed', queue: queue})
                    callback(false)
                    return

                callback(true)
                return

        return

    # ------------------------------------------------------------------------


    client = null
    writeClient = null
    writeClientReady = false
    connected = false
    shutdownRequested = false
    self.running = false
    self.busy = false
    startingUpPhase = false
    self.on 'next', runNextJob

    initClients = ()->
        client = new fivebeans.client(beanstalkHost, beanstalkPort)
        client.on 'connect', clientConnected
        client.on 'error', clientErrored
        client.on 'close', clientClosed

        writeClient = new fivebeans.client(beanstalkHost, beanstalkPort)
        writeClientReady = false
        writeClient.on 'connect', writeClientConnected
        writeClient.on 'connect', ()->
            # connect the read client AFTER the write client
            startingUpPhase = false
            client.connect()
            return
        writeClient.on 'error', writeClientErrored
        writeClient.on 'close', writeClientClosed
        return

    self.getId = ()->
        return workerId
    
    self.run = ()->
        logger.silly("launching worker #{workerId}")
        client = null
        writeClient = null
        shutdownRequested = false
        self.running = true
        startingUpPhase = true
        initClients()
        writeClient.connect()
        return

    self.stop = ()->
        shutdownRequested = true
        return
    
    return self

module.exports = exports