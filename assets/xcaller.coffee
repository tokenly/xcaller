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
SILLY                     = !!(process.env.SILLY                  or false)
NOTIFICATIONS_OUT_TUBE    = process.env.NOTIFICATIONS_OUT_TUBE    or 'notifications_out'
NOTIFICATIONS_RETURN_TUBE = process.env.NOTIFICATIONS_RETURN_TUBE or 'notifications_return'
JOB_LOG_FILENAME          = process.env.JOB_LOG_FILENAME          or null
VERSION                   = '0.3.0'

figlet      = require('figlet')
winston     = require('winston')
Worker      = require('./lib/worker')
EventEmitter = require('events').EventEmitter



MAX_SHUTDOWN_DELAY = CLIENT_TIMEOUT + 1000  # <-- when shutting down, never wait longer than this for a response from any client


# ------------------------------------------------------------------------
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
    level: if SILLY then 'silly' else if DEBUG then 'debug' else 'info'
})
if JOB_LOG_FILENAME
    translateLevel = (levelString)->
        map = {
            silly:     50
            debug:     100
            verbose:   150
            info:      200
            warn:      300
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

# ------------------------------------------------------------------------
# build workers

running = false
workerEvents = new EventEmitter()
workers = []
createWorkers = ()->
    for i in [0...MAX_QUEUE_SIZE]
        logger.silly("creating worker #{i}")
        worker = Worker.buildWorker({
            workerId:      i,
            beanstalkHost: beanstalkHost,
            beanstalkPort: beanstalkPort,
            outTube:       NOTIFICATIONS_OUT_TUBE,
            returnTube:    NOTIFICATIONS_RETURN_TUBE,
            clientTimeout: CLIENT_TIMEOUT,
            maxRetries:    MAX_RETRIES,
            logger:        logger,
        })
        worker.on 'closed', (closedWorkerId)->
            logger.silly "worker closed", {id: closedWorkerId}
            handleClosedWorker(closedWorkerId)
        workers.push(worker)

handleClosedWorker = (i)->
    if running
        logger.silly("will restart closed worker #{i}")
        setTimeout ()->
            logger.silly("restarting closed worker #{i}")
            restartWorker(i)
        , 5000


restartWorker = (i)->
    if running
        workers[i].run()
    return

startWorker = (i)->
    logger.silly("start worker #{i}")
    workers[i].run()
    return

startWorkers = ()->
    for i in [0...MAX_QUEUE_SIZE]
        if running
            startWorker(i)
    return

runAllWorkers = ()->
    running = true
    startWorkers()
    return

stopAllWorkers = ()->
    running = false
    for i in [0...MAX_QUEUE_SIZE]
        workers[i].stop()
    return

getNumberOfWorkersRunning = ()->
    count = 0
    for i in [0...MAX_QUEUE_SIZE]
        if workers[i].running
            ++count
    return count

getNumberOfWorkersBusy = ()->
    count = 0
    for i in [0...MAX_QUEUE_SIZE]
        if workers[i].busy
            ++count
    return count


# ------------------------------------------------------------------------
# install signal handlers

gracefulShutdown = (callback)->
    startTimestamp = new Date().getTime()
    if DEBUG then console.log "[#{new Date().toString()}] begin shutdown"
    stopAllWorkers()

    intervalReference = null
    runShutdown = ()->
        numberOfWorkersRunning = getNumberOfWorkersRunning()
        if numberOfWorkersRunning == 0 or (new Date().getTime() - startTimestamp >= MAX_SHUTDOWN_DELAY)
            if numberOfWorkersRunning > 0
                if DEBUG then console.log "[#{new Date().toString()}] Gave up waiting on #{numberOfWorkersRunning} workers(s)"
            if DEBUG then console.log "[#{new Date().toString()}] shutdown complete"
            if intervalReference? then clearInterval(intervalReference)
            if busyIntervalRef?   then clearInterval(busyIntervalRef)
            callback()
        else
            if DEBUG then console.log "[#{new Date().toString()}] waiting on #{numberOfWorkersRunning} worker(s)"
        return


    intervalReference = setInterval(runShutdown, 350)
    runShutdown()
    return



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


# ------------------------------------------------------------------------
# track busy level

runBusyReport = ()->
    numberOfWorkersRunning = getNumberOfWorkersBusy()
    logger.info("Using #{numberOfWorkersRunning} of #{MAX_QUEUE_SIZE} workers", {name: "usageReport", used: numberOfWorkersRunning, max: MAX_QUEUE_SIZE})
    return
BUSY_REPORT_INTERVAL_TIME = 30 # <- seconds
busyIntervalRef = setInterval(runBusyReport, BUSY_REPORT_INTERVAL_TIME * 1000)


# ------------------------------------------------------------------------
# run the workers

figlet.text "Tokenly XCaller", 'Slant', (err, data)->
    process.stdout.write "#{data}\n\nVersion #{VERSION}\nconnecting to beanstalkd at #{beanstalkHost}:#{beanstalkPort}\n\n"
    return

setTimeout ()->
    logger.debug('start server', {name: 'lifecycle.start', })
    createWorkers()
    runAllWorkers()
, 10


