"use strict";

/*
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE. 
*/

//------------------------------------------------------------------------------
// Require statements and vars
//------------------------------------------------------------------------------
var  fs = require('fs');
var Q = require('q');
var json2csv = require('json2csv');

// Global vars
var startTime = new Date();;
var startMilli = startTime.getTime();

console.log('\n');
var osys = process.platform;
var home = process.env.HOME || process.env.HOMEPATH || process.env.USERPROFILE
console.log('cpul000i - Runtime OpSys: ' + osys + '  Home: ' + home);
var cfParm = false;
var configFileName = 'config.json'
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

//------------------------------------------------------------------------------
// read parms from program start command
//------------------------------------------------------------------------------
// Display the program start parameters and name of the configuration file that is to be used
process.argv.forEach(function (val, index, array) {
    console.log('cpul001i - Start Parameter: ' + index + ': ' + val);
    if (index === 2) {
        cfParm = true;
        if (val.startsWith('/')) {
            configFileName = val;
        } else if (val.startsWith('~')) {
            configFileName = home + val.substring(1);
        } else if (osys === 'win32' || osys === 'win64') {
            if (val.indexOf(":") === -1) {
                configFileName = './' + val;
            } else  {
                configFileName = val;
            }
        } else if (val.startsWith('./')) {
            configFileName = val;
        } else {
            configFileName = './' + val;
        }
        console.log('cpul002i - Loading parameters from configuration file: ' + configFileName);
    }
});

// if no configuration file parameter, display message about using default file name
if (!cfParm) {
    console.log('cpul003i - Loading parameters from DEFAULT configuration file: config.json');
}

initProgram();

function initProgram() {
    // read config file and get parameters to execute
    getConfig()
        .then(function(result) {
            if (typeof runConfig === 'object') {
                // read the config.json
                loadVars();
                if (!failedParms) {
                    // initalize the input database 
                    initDBConnections();
                    if (!failedToInit) {
                        console.log('cpul004i - Retrieving data from database');
                        getDocs();
                    } else {
                      console.log('cpul005e - Program terminated');
                      process.exit(-1);
                    }
                } else {
                  console.log('cpul006e - Program terminated');
                  process.exit(-1);
                }
            }
        })
        .catch(function(err) {
            console.log('cpul007e - FAILED to process parm file, msg: ' + err);
            process.exit(-1);
        });
}

function initDBConnections() {
     failedToInit = false;  
        try {
                databaseDef = require('cloudant')(runConfig.database.url); 
                db  = databaseDef.use(runConfig.database.dbname);
                console.log('cpul011i - Initialized DB: ' + runConfig.database.dbname);
        } catch (e) {
                console.log('cpul012e - Error initialinzing database, message: ' + e);
                failedToInit = true;
        }
  }

function endStats() {
        var endTime = new Date();
        var endMilli = endTime.getTime();
        var milli = endMilli - startMilli;
        var totalTime = millisecondsToTime(milli)
        var avgTimePerRec = milli / totalPulled;

        console.log('cpul909i - Start date/time           : ' + startTime);
        console.log('cpul901i - End date/time             : ' + endTime);
        console.log('cpul902i - Elapsed time              : ' + totalTime + ' (MM:SS.mmm)');
        console.log('cpul903i - Average Time/Record       : ' + avgTimePerRec + ' milliseconds');

        // end the load program
        console.log('cpul999i - Program completed\n');
}

//------------------------------------------------------------------------------
// format milliseconds to MM:SS.mmm
//------------------------------------------------------------------------------
function millisecondsToTime(milli) {
    var milliseconds = milli % 1000;
    var seconds = Math.floor((milli / 1000) % 60);
    var minutes = Math.floor((milli / (60 * 1000)) % 60);
    return minutes + ":" + seconds + "." + milliseconds;
}


function getDocs() {       
        getBatch(batchsize, skip)
        .then(function(result) {
            if (typeof dbData[0] !== 'undefined') {
                convert()
                .then(function(result) {
                    skip = skip + batchsize;
                    dbData = [];
                    console.log('cpul400i - Total database records pulled: ' + totalPulled);
                    getDocs();
                })
            } else {
                endStats();
            }
        })
        .catch(function(err) {
            console.log('cpul401e - Error getting docs from database, message: ' + err);
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
            console.log('cpul402e - Error pulling data from database, message: ' + e);
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
                console.log('cpul403i - Output file opened')
            })
            .catch(function() {
                console.log('cpul404e - Failed to open output file, program terminated');
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
            fs.appendFile(fname, csv, function(err) {
                if (err) { 
                    console.log('cpul406e - Error writing data to output file, message: ' + err)
                    deferred.reject('FAIL');
                } else {
                    deferred.resolve('OK'); 
                }
            });
            return deferred.promise;
        } catch (e) {
            console.log('cpul407e - Error initialinzing database, message: ' + e);
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
                    if (err.code === 'ENOENT') { 
                            console.log('cpul408i - Cleared outupt file ' + fname);
                            deferred.resolve('OK'); 
                    } else {
                            console.log('cpul409e - Error clearing output file, message: ' + err)
                            deferred.reject('FAIL');
                }
            } else {
                console.log('cpul410i - Cleared outupt file ' + fname);
                deferred.resolve('OK'); 
            }
        });
        return deferred.promise;
    } catch (e) {
         console.log('cpul411e - Error clearing output file, message: ' + e);
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
            console.log('cpul200i - Parameter \"database\" located');
        } else {
            console.log('cpul201e - Parameter \"database\" parameter is missing, required to process');
            failedParms = true;
        }

        // Validate database.dbname
        if (typeof runConfig.database.dbname !== 'undefined') {
            console.log('cpul202i - Parameter \"database.dbname\" value is: ' + runConfig.database.dbname);
        } else {
            console.log('cpul203e - Parameter \"database.dbname\" is missing, required to process');
            failedParms = true;
        }
        
        // Validate database.url
        if (typeof runConfig.database.url !== 'undefined') {
            console.log('cpul204i - Parameter \"database.url\" value is: ' + runConfig.database.url);
        } else {
            console.log('cpul205e -Parameter \"database.url\" is missing, required to process');
            failedParms = true;
        }

        // Validate outputFile
        if (typeof runConfig.outputFile !== 'undefined') {
            console.log('cpul206i - Parameter \"outputFile\" value is: ' + runConfig.outputFile);
        } else {
            console.log('cpul207e - Parameter \"outputFile\" is missing, required to process');
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
            console.log('cpul208i - Parameter \"delimiter\" value is: ' + msg)
        } else {
            console.log('cpul209i - Parameter \"delimiter\" is not provided, using default comma');
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
            console.log('cpul210i - Parameter \"quote\" value is: ' + msg)
        } else {
            console.log('cpul211i - Parameter \"quote\" is not provided, using default double');
            quote = '"';
         }

         // Validate fields
        if (typeof runConfig.fields !== 'undefined') {
          var tmp = runConfig.fields
          var hl = tmp.length;
          if (hl === 0 ) {
              console.log('cpul212e - Parameter \"field\" definition located in \"fields\" parameter, required to process');
              failedParms = true;
          } else {
            var p = 0;
            for (var i = 0; i < hl; i++) {
              if (typeof tmp[i].field === 'undefined') {
                p = i + 1;
                console.log('cpul213e - Parameter \"fields\" entry ' + p + ' does not contain \"field\" definition, required to process');
                failedParms = true;
              }
            }
            console.log('cpul214i - Parameter \"fields\" value is: ' + JSON.stringify(runConfig.fields));
          }
        } else {
            console.log('cpul215e - Parameter \"fields\" is missing, required to process');
            failedParms = true;
        }

        if (typeof runConfig.batchsize !== 'undefined') {
            if (typeof runConfig.batchsize === 'number') {
                if (runConfig.batchsize === 0) {
                    batchsize = 5000;
                    console.log('cpul216i - Parameter \"batchsize\" defined as zero, will be set to default 5000');
                } else {
                    batchsize = runConfig.batchsize;
                    console.log('cpul217i - Parameter \"batchsize\" value is: ' + batchsize);
                }
            } else {
                batchsize = 5000;
                console.log('cpul218i - Parameter \"batchsize\" not defined as number, will be reset to default 5000');
            }
        } else {
            console.log('cpul219i - Parameter \"batchsize\" not defined using default value: 5000');
            batchsize = 5000;
        }


      } catch (e) {
      database = null;
      console.log('cpul1240e - Error processing configuration file, message: ' + e);
    }
}


//------------------------------------------------------------------------------
// read configuration file
//------------------------------------------------------------------------------
function getConfig() {
    var deferred = Q.defer();
    try {
        fs.readFile(configFileName, "utf8", function(err, data) {
            if (err) {
                if (err.code === 'ENOENT') {
                    console.log('cpul300i - Configuration file: ' + configFileName + '  does not exist, no SMS will be configured');
                    deferred.reject('ENOENT');
                }
                if (err.code === 'EACCES') {
                    console.log('cpul301e - Configuration file: ' + configFileName + '  has Permission error');
                    deferred.reject('EACCES');
                }
                console.log('cpul302e - Unknown Error reading configuration file: ' + configFileName + ', message: ' + err);
                deferred.reject('UNKERR');
            }
            // array of config parms

            try {
                runConfig = JSON.parse(data);
            } catch (e) {
                console.log('cpul303e - Invalid format in configuration file, message: ' + e);
                runConfig = '';
                deferred.reject('FORMAT_ERROR');
            }
            deferred.resolve('OK');
        });

        return deferred.promise;

    } catch (e) {
        console.log('cpul304e - Error reading configuration file: config.json, message: ' + e);
        deferred.reject(e);
    }
}
