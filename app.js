/**
 * Module dependencies.
 */
var  fs           = require('fs');
var Q             = require('q');
var json2csv = require('json2csv');

//------------------------------------------------------------------------------
// Variables 
//------------------------------------------------------------------------------
var db;
var databaseDef;
var runConfig = null;
var failedToInit = false;
var failedParms = false;
var dbData = [];
var delimiter = ',';
var quote = '"';
var batchsize = 5000;
var skip = 0;
var cnt = 0;
var firstTime = true;
var totalPulled = 0;

initProgram();

function initProgram() {
    // read config file and get parameters to execute
    getConfig()
        .then(function(result) {
            console.log('pull000i - Looking for configuration file');
            if (typeof runConfig === 'object') {
                // read the config.json
                loadVars();
                if (!failedParms) {
                    // initalize the input database 
                    initDBConnections();
                    if (!failedToInit) {
                        console.log('pull080i - Retrieving data from database');
                        getDocs();
                    } else {
                      console.log('pull018e - Program terminated');
                      process.exit(-1);
                    }
                } else {
                  console.log('pull001e - Program terminated');
                  process.exit(-1);
                }
            }
        })
        .catch(function(err) {
            console.log('pull001e - FAILED to retrieve parm file, msg: ' + err);
            return;
        });
}

function initDBConnections() {
     failedToInit = false;  
        try {
                databaseDef = require('cloudant')(runConfig.database.url); 
                db  = databaseDef.use(runConfig.database.dbname);
                console.log('pull012i - Initialized DB: ' + runConfig.database.dbname);
        } catch (e) {
                console.log('pull1240e - Error initialinzing database, message: ' + e);
                failedToInit = true;
        }
  }


function getDocs() {       
        getBatch(batchsize, skip)
        .then(function(result) {
            if (typeof dbData[0] !== 'undefined') {
                convert()
                .then(function(result) {
                    skip = skip + batchsize;
                    dbData = [];
                    console.log('pull400i - Total database records pulled: ' + totalPulled);
                    getDocs();
                })
            } else {
                console.log('pull006i - Data pull complete');
                process.exit(0);
            }
        })
        .catch(function(err) {
            console.log('pull019e - Error getting docs from database, message: ' + err);
        })
}


function getBatch(batchsize, skip) {
        cnt = 0;
        var deferred = Q.defer();
        var params = {
            include_docs: true,
            limit: batchsize,
            skip: skip
        } 
    
        try { 
            db.list(params, function(err, body, headers) {
                if (!err) {
                    body.rows.forEach(function(data) {
                        cnt++;
                        totalPulled++;
                        dbData.push(data.doc);
                    });
                    deferred.resolve('OK'); 
                } else {
                    deferred.reject(err);
                }
            });
            return deferred.promise;      
        } catch (e) {
            console.log('pull099e - Error initialinzing database, message: ' + e);
            failedToInit = true;
            deferred.reject(e);
        }
}


function convert() {
        var deferred = Q.defer();
        var tmp = runConfig.fields;
        var hl = tmp.length;
        var pick = [];
        var msg = '';
        var fld;
        var fname = runConfig.outputFile;
        var wrtHdr;
        
        // determine if column header names shoudl be written    
        if (firstTime) {
            wrtHdr = true;
            firstTime = false;
            clearFile()
            .then(function(){
                console.log('pull004i - Output file opened')
            })
            .catch(function() {
                console.log('pull005e - Failed to open output file, program terminated');
                process.exit(-1);
            });
        } else {
            wrtHdr = false;
        }

        // build fields list
        for (var i = 0; i < tmp.length; i++) {
            fld = tmp[i].field;
            pick[i] = fld;
            if (i === 0) {
            msg = fld;
            } else {
            msg = msg + ', ' + fld;        
            }
        }

        // build options for json2csv
        var opts = {
            data: dbData,
            fields: pick,
            quotes: quote,
            del: delimiter,
            hasCSVColumnTitle: wrtHdr
        };

        // convert JSON data to delimited data
        var csv = json2csv(opts);

        try {
            //console.log('pull020i - Writing data to file: ' + fname + ' with fields: ' + msg)  
            fs.appendFile(fname, csv, function(err) {
                if (err) { 
                    console.log('pull021e - Error writing data to output file, message: ' + err)
                    deferred.reject('FAIL');
                } else {
                    //console.log('pull022i - Appended data to output file ');
                    deferred.resolve('OK'); 
                }
            });
            return deferred.promise;
        } catch (e) {
            console.log('pull050e - Error initialinzing database, message: ' + e);
            failedToInit = true;
            return deferred.reject(e);
        }
}

function clearFile() {
    var fname = runConfig.outputFile;
    var data = '';
    var deferred = Q.defer();
    try {
        fs.unlink(fname, function(err) {
            if (err) { 
                console.log('pull040e - Error clearing output file, message: ' + err)
                deferred.reject('FAIL');
            } else {
                console.log('pull041i - Cleared outupt file ' + fname);
                deferred.resolve('OK'); 
            }
        });
        return deferred.promise;
    } catch (e) {
         console.log('pull042ee - Error clearing output file, message: ' + e);
        return deferred.reject(e);
    }
}

//------------------------------------------------------------------------------
// read config.json and build / set local vars
//------------------------------------------------------------------------------
function loadVars() {
    failedToInit = false;
    try {
        // Validate database defined
        if (typeof runConfig.database !== 'undefined') {
            console.log('pull002i - Parameter \"database\" located');
        } else {
            console.log('pull012e - Parameter \"database\" parameter is missing, required to process');
            failedParms = true;
        }

        // Validate database.dbname
        if (typeof runConfig.database.dbname !== 'undefined') {
            console.log('pull002i - Parameter \"database.dbname\" value is: ' + runConfig.database.dbname);
        } else {
            console.log('pull012e - Parameter \"database.dbname\" is missing, required to process');
            failedParms = true;
        }
        
        // Validate database.url
        if (typeof runConfig.database.url !== 'undefined') {
            console.log('pull002i - Parameter \"database.url\" value is: ' + runConfig.database.url);
        } else {
            console.log('pull012e -Parameter \"database.url\" is missing, required to process');
            failedParms = true;
        }

        // Validate outputFile
        if (typeof runConfig.outputFile !== 'undefined') {
            console.log('pull002i - Parameter \"outputFile\" value is: ' + runConfig.outputFile);
        } else {
            console.log('pull012e - Parameter \"outputFile\" is missing, required to process');
            failedParms = true;
        }

        // Validate delimiter
        if (typeof runConfig.delimiter !== 'undefined') {
            var data = runConfig.delimiter;
            var msg = '';
            data = data.toUpperCase();

            if (data === "TAB") {
              delimiter = '\t';
              msg = 'tab'
            } else if (data === "COMMA") {
              delimiter = ',';
              msg = 'comma'
            } else {
              delimiter = data;
              msg = data;
            }
            console.log('pull002i - Parameter \"delimiter\" value is: ' + msg)
        } else {
            console.log('pull003i - Parameter \"delimiter\" is not provided, using default comma');
            delimiter = ',';
         }

        // Validate quote
        if (typeof runConfig.quote !== 'undefined') {
            var data = runConfig.quote;
            var msg = '';
            data = data.toUpperCase();

            if (data === "SINGLE" || data === "'") {
              quote = "'";
              msg = 'single'
            } else if (data === "DOUBLE" || data === '"') {
              quote = '"';
              msg = 'double'
            } else {
              quote = data;
              msg = data;
            }
            console.log('pull002i - Parameter \"quote\" value is: ' + msg)
        } else {
            console.log('pull003i - Parameter \"quote\" is not provided, using default double');
            quote = '"';
         }

         // Validate fields
        if (typeof runConfig.fields !== 'undefined') {
          var tmp = runConfig.fields
          var hl = tmp.length;
          if (hl === 0 ) {
              console.log('pull012e - Parameter \"field\" definition located in \"fields\" parameter, required to process');
              failedParms = true;
          } else {
            var p = 0;
            for (var i = 0; i < hl; i++) {
              if (typeof tmp[i].field === 'undefined') {
                p = i + 1;
                console.log('pull012e - Parameter \"fields\" entry ' + p + ' does not contain \"field\" definition, required to process');
                failedParms = true;
              }
            }
            console.log('pull002i - Parameter \"fields\" value is: ' + JSON.stringify(runConfig.fields));
          }
        } else {
            console.log('pull012e - Parameter \"fields\" is missing, required to process');
            failedParms = true;
        }


      } catch (e) {
      database = null;
      console.log('pull1240e - Error processing configuration file, message: ' + e);
    }
}


//------------------------------------------------------------------------------
// read configuration file
//------------------------------------------------------------------------------
function getConfig() {
    var deferred = Q.defer();
    try {
        fs.readFile('./config.json', "utf8", function(err, data) {
            if (err) {
                if (err.code === 'ENOENT') {
                    console.log('pull013i - Configuration file: config.json does not exist, no SMS will be configured');
                    deferred.reject('ENOENT');
                }
                if (err.code === 'EACCES') {
                    console.log('pull014e - Configuration file: config.json has Permission error');
                    deferred.reject('EACCES');
                }
                console.log('pull015e - Unknown Error reading configuration file: config.json');
                deferred.reject('UNKERR');
            }
            // array of config parms

            try {
                runConfig = JSON.parse(data);
            } catch (e) {
                console.log('pull016e - Invalid format in config.json configuration file, message: ' + e);
                deferred.reject(e);
                runConfig = '';
            }
            deferred.resolve('OK');
        });

        return deferred.promise;

    } catch (e) {
        console.log('pull017e - Error reading configuration file: config.json, message: ' + e);
        deferred.reject(e);
    }
}
