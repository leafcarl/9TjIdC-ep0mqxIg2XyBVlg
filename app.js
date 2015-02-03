/*
 *  This currency extractor is only for experimental purpose.
 *  Automated extraction may violet rules at target web.
 */

/*
 *  Module list
 *  request: http requests
 *  cheerio: jQuery like for data scrapping
 *  mongodb: native nodejs client for MongoDB
 *  fivebeans: beanstalkd client
 */
var request = require('request');
var cheerio = require('cheerio');
var mongo_client = require('mongodb').MongoClient;
var fivebeans = require('fivebeans');

/*
 *  Constant values
 *  target_action_url: XE convertor url
 *  tube: tube name at beanstalkd server
 */
var target_action_url = 'http://www.xe.com/currencyconverter/convert/';
var tube = 'leafcarl';

/*
 *  Request option object for getting beanstalkd host at Aftership
 */
var aftership_beanstalkd_request_option = {
    method: 'POST',
    url: 'http://challenge.aftership.net:9578/v1/beanstalkd',
    headers: {
        'aftership-api-key': 'a6403a2b-af21-47c5-aab5-a2420d20bbec'
    }
};

/*
 *  Job constant with priority 1, delay 60s, allowed time-to-run 10s, dealy when failure 3s
 */
var job_const = {
    priority: 1,
    delay: 60,
    ttr: 10,
    delay_failure: 3,
    failure_threshold: 10
};

/*
 *  Database API provide connect, insert and close call
 *  Setup:
 *  - mongo_shell: MongoDB host(e.g. mongodb://xxx)
 *  - collection: target collection to be used at the Mongo host
 */
var database = {

    mongo_shell: 'mongodb://beanstalk:iqbox@ds029630.mongolab.com:29630/aftership_challenge3',

    collection: 'currencies',

    db: null,

    /*
     *  connect to db
     *  @para function(): callback function
     */
    connect: function(callback) {
        mongo_client.connect(this.mongo_shell, function(err, db) {
            if (err) throw err;
            this.db = db;
            callback();
        });
    },

    /*
     *  insert to db
     *  @para string: target collection to be used at the Mongo host
     *  @para string: serialized document for insert
     *  @para function(Object): callback function
     */
    insert: function(doc, callback) {
        db.collection(this.collection).insert(doc, function(err, result) {
            if (err) throw err;
            callback(result);
        });
    },

    /*
     *  close db connection
     */
    close: function() {
        db.close();
    }
};

/*
 *  Fivebeans beanstalkd client
 */
var client;

/*
 *  Counter for number of failure
 */
var failure_counter = 0;

/*
 *  Scrap currency value from the web
 *  @para string: base currency unit
 *  @para string: target currency unit
 *  @para function(string, string): callback function
 */
var currencyScrap = function(from_currency, to_currency, callback) {
    // generate request detail with query parameters and header
    var request_options = {
        url: target_action_url + '?Amount=1&From=' + from_currency + '&To=' + to_currency,
        headers: {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.8,zh-TW;q=0.6,zh;q=0.4',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.94 Safari/537.36'
        }
    };
    // http request and currency value extraction
    request(request_options, function(err, response, body) {
        var error;
        if (err || response.statusCode != 200)
            error = 'XE target unreachable';
        else {
            var $ = cheerio.load(body);
            var $currency_part = $('.uccResult .ucc-result-table .rightCol');
            var currency_match = $currency_part.text().trim().match(/[+-]?\d+\.\d+/);
            if (currency_match == null || !(currency_match > 0)) {
                // value not found or invalid value
                error = 'Currency value not found';
            } else {
                var currency_string = parseFloat(currency_match[0]).toFixed(2).toString();
            }
        }
        callback(error, currency_string);
    });
};

/*
 *  Consume ready job from beanstalkd server
 */
var consumer = function() {
    // reserve a ready job
    client.reserve(function(err, jobid, payload) {
        try {
            // extract payload
            var payload_obj = JSON.parse(payload);
            if (payload_obj.from == null || payload_obj.to == null) {
                throw new SyntaxError('Payload mismatch');
            }
            // scrap currency value
            currencyScrap(payload_obj.from, payload_obj.to, function(error, currency_string) {
                var job_delay = job_const.delay;
                if (!error) {
                    // insert to db
                    database.insert({
                        'from': payload_obj.from,
                        'to': payload_obj.to,
                        'created_at': new Date(),
                        'rate': currency_string
                    }, function(result) {
                        // callback after insert to db
                        if (result.result.ok == 1 && result.result.n == 1) {
                            console.log('Insert to collection at ' + new Date());
                        } else {
                            client.quit();
                            throw new Error('Insertion error');
                        }
                    });
                    failure_counter = 0;
                } else {
                    // target web error
                    console.log(++failure_counter + ' target web error: ' + error + ' at ' + new Date());
                    if (failure_counter >= job_const.failure_threshold) {
                        client.quit();
                        throw 'Fail for 10 trials'
                    }
                    job_delay = job_const.delay_failure;
                }
                // reput the job into delay queue
                client.release(jobid, job_const.priority, job_delay, function(err) {
                    // callback after job release
                    if (err) {
                        // NOT_FOUND might be caused by short ttr
                        if (err == 'NOT_FOUND')
                            throw err + ' for jobid ' + jobid + ', try to set more ttr';
                        client.quit();
                        throw err + ' for jobid ' + jobid;
                    }
                    consumer();
                });
            });
        } catch (e) {
            if (e instanceof SyntaxError) {
                // payload error
                client.bury(jobid, job_const.priority, function(err) {
                    consumer();
                });
                console.log('Payload syntaxError');
            } else {
                // rethrow other exceptions
                client.bury(jobid, job_const.priority, function(err) {});
                client.quit();
                throw e;
            }
        }
    });
};

/*
 *  Request for the beanstalkd host and start the beanstalkd client
 */
request(aftership_beanstalkd_request_option, function(err, response, body) {
    if (err || response.statusCode != 200) {
        throw 'Aftership Beanstalkd server request fail';
    }
    // convert from string to JSON object
    var body_obj = JSON.parse(body);
    if (body_obj.meta.code == 200) {
        console.log('Beanstalkd server: ' + body_obj.data.host + ':' + body_obj.data.port);
        // connect and start the beanstalkd client
        client = new fivebeans.client(body_obj.data.host, body_obj.data.port);
        client
            .on('connect', function() {
                // client can now be used
                console.log('fivebeans connected');
                database.connect(function() {
                    // db connect success
                    client.use(tube, function(err, tubename) {
                        client.watch(tube, function(err, numwatched) {
                            // start consumer
                            consumer();
                        });
                    });
                });
            })
            .on('error', function(err) {
                // connection failure
                throw err;
            })
            .on('close', function() {
                // underlying connection has closed
                console.log('fivebeans closed');
                database.close();
            }).connect();
    } else {
        // response with error code
        console.log('Beanstalkd server status ' + body_obj.meta.code + ' ' + body_obj.meta.type + ' msg: ' + body_obj.meta.message);
    }
});