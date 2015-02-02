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
 *  mongo_shell: MongoDB host(e.g. mongodb://xxx)
 *  collection_name: target collection to be used at the Mongo host
 *  target_action_url: XE convertor url
 *  tube: tube name at beanstalkd server
 */
var mongo_shell = 'mongodb://beanstalk:iqbox@ds029630.mongolab.com:29630/aftership_challenge3';
var collection_name = 'currencies';
var target_action_url = 'http://www.xe.com/currencyconverter/convert/';
var tube = 'leafcarl';

/*
 *  Job constant with priority 1, delay 60s, allowed time-to-run 20s, dealy when failure 3s
 */
var job_const = {
    priority: 1,
    delay: 60,
    ttr: 20,
    delay_failure: 3
}

var client = new fivebeans.client('challenge.aftership.net', 11300);

/*
 *  Database API provide connect, insert and close call
 */
var database = {
    db: null,

    /*
     *  connect to db
     *  @para function(): callback function
     */
    connect: function(callback) {
        mongo_client.connect(mongo_shell, function(err, db) {
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
    insert: function(collection_name, doc, callback) {
        db.collection(collection_name).insert(doc, function(err, result) {
            if (err) throw err;
            callback(result);
        });
    },

    /*
     *  close db
     */
    close: function() {
        db.close();
    }
}

/*
 *  Scrap currency value from the web
 *  @para string: base currency unit
 *  @para string: target currency unit
 *  @para function(string): callback function
 */
var currencyScrap = function(from_currency, to_currency, callback) {
    // generate request detail and header
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
        if (err || response.statusCode != 200) throw 'Target unreachable';
        var $ = cheerio.load(body);
        var $currency_part = $('.uccResult .ucc-result-table .rightCol');
        var currency_match = $currency_part.text().trim().match(/[+-]?\d+\.\d+/);
        if (currency_match == null) throw 'Currency not found';
        var currency_string = parseFloat(currency_match[0]).toFixed(2).toString();
        callback(currency_string);
    });
}

/*
 *  Submit a job to beanstalkd server
 *  @para number: job delay in seconds
 *  @para string: job payload
 */
var putJob = function(delay, payload) {
    client.put(job_const.priority, delay, job_const.ttr, payload, function(err, jobid) {
        console.log(jobid);
    });
}

/*
 *  Consume ready job from beanstalkd server
 */
var consumeJob = function() {
    console.log('consumeJob()');
    // reserve a ready job
    client.reserve(function(err, jobid, payload) {
        console.log('reserve job: ' + jobid);
        try {
            // extract payload
            var payload_obj = JSON.parse(payload);
            if (payload_obj.from == null || payload_obj.to == null) {
                throw new SyntaxError('Payload mismatch');
            }
            // scrap currency value
            currencyScrap(payload_obj.from, payload_obj.to, function(currency_string) {
                // insert to db
                database.insert(collection_name, {
                    'from': payload_obj.from,
                    'to': payload_obj.to,
                    'created_at': new Date(),
                    'rate': currency_string
                }, function(result) {
                    if (result) console.log('Insert to collection');
                });
            });
            // destroy the ready job
            client.destroy(jobid, function(err) {});
            // put a new job with the same payload
            putJob(job_const.delay, payload);
        } catch (e) {
            if (e instanceof SyntaxError) {
                // payload error
                client.bury(jobid, job_const.priority, function(err) {});
                console.log('SyntaxError');
            } else if (e == 'Target unreachable' || e == 'Currency not found') {
                // target web error
                client.destroy(jobid, function(err) {});
                // put a new job with shorter delay and the same payload                
                putJob(job_const.delay_failure, payload);
                console.log('Target web error');
            }
        } finally {
            // consume next job
            consumeJob();
        }
    });
}

/*
 *  connect and start the beanstalkd client
 */
client
    .on('connect', function() {
        // client can now be used
        console.log('fivebeans connected');
        database.connect(function() {
            client.use(tube, function(err, tubename) {
                client.watch(tube, function(err, numwatched) {
                    console.log(numwatched);
                    consumeJob();
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
    })
    .connect();