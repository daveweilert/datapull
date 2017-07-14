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
var inputDBDef;
var runConfig = null;
var failedToInit = false;
var failedParms = false;
var cnt = 0;
var dbData = [];

initProgram();

function initProgram() {
    // read config file and get parameters to execute
    getConfig()
        .then(function(result) {
            console.log('pull0000i - Looking for configuration file');
            if (typeof runConfig === 'object') {
                // read the config.json
                loadVars();
                if (!failedParms) {
                    // initalize the input database 
                    initDBConnections();
                    if (!failedToInit) {
                        console.log('pull4000i - Retrieving data from database');
                        getDocs()
                            .then(function(result) {
                              console.log('pull4001i - DB Record count: ' + cnt);
                              convert();
                            })
                            .catch(function(err) {
                              console.log('pull0019e - Error getting docs from database, message: ' + err);
                            })
                    } else {
                      console.log('pull0018e - Program terminated');
                      process.exit(-1);
                    }
                } else {
                  console.log('pull0001e - Program terminated');
                  process.exit(-1);
                }
            }
        })
        .catch(function(err) {
            console.log('pull0001e - FAILED to retrieve parm file, msg: ' + err);
            return;
        });
}

function initDBConnections() {
  failedToInit = false;  
  try {
    		inputDBDef = require('cloudant')(runConfig.inputDB.url), params   = {include_docs: true}  ; 
	      db  = inputDBDef.use(runConfig.inputDB.dbname);
        console.log('pull0012i - Initialized DB: ' + runConfig.inputDB.dbname);
    } catch (e) {
        console.log('pull1240e - Error initialinzing database, message: ' + e);
        failedToInit = true;
    }
  }

function getDocs() {
    cnt = 0;
    var deferred = Q.defer();

    try { 
        db.list(params, function(err, body, headers) {
            if (!err) {
                body.rows.forEach(function(data) {
                    cnt++;
                    dbData.push(data.doc);
                });
            }
            deferred.resolve('OK'); 
          });
          return deferred.promise;      
    } catch (e) {
        console.log('pull1240e - Error initialinzing database, message: ' + e);
        failedToInit = true;
        deferred.reject(e);
    }
}

function convert() {
    var tmp = runConfig.fields;
    var hl = tmp.length;
    var pick = [];
    var msg = '';
    var fld;
    var fname = runConfig.outputFile;
 
    for (var i = 0; i < tmp.length; i++) {
        fld = tmp[i].field;
        pick[i] = fld;
        if (i === 0) {
          msg = fld;
        } else {
          msg = msg + ' ' + fld;        
        }
    }
    console.log('pull0020i - Output to CSV file: ' + fname + '  -  fields: ' + msg)  

    var csv = json2csv({ data: dbData, fields: pick });

    fs.writeFile(fname, csv, function(err) {
        if (err) { 
            console.log('pull0021e - Error creating output file, message: ' + err)
            throw err; 
        } else {
            console.log('pull00221 - Output complete ');
        }
    });
}


//------------------------------------------------------------------------------
// read config.json and build / set local vars
//------------------------------------------------------------------------------
function loadVars() {
    failedToInit = false;
    try {
        // Validate inputDB defined
        if (typeof runConfig.inputDB !== 'undefined') {
            console.log('pull0002i - Parameter \"inputDB\" located');
        } else {
            console.log('pull0012e - Parameter \"inputDB\" parameter is missing, required to process');
            failedParms = true;
        }

        // Validate inputDB.dbname
        if (typeof runConfig.inputDB.dbname !== 'undefined') {
            console.log('pull0002i - Parameter \"inputDB.dbname\" ' + runConfig.inputDB.dbname);
        } else {
            console.log('pull0012e - Parameter \"inputDB.dbname\" is missing, required to process');
            failedParms = true;
        }
        
        // Validate inputDB.url
        if (typeof runConfig.inputDB.url !== 'undefined') {
            console.log('pull0002i - Parameter \"inputDB.url\" ' + runConfig.inputDB.url);
        } else {
            console.log('pull0012e -Parameter \"inputDB.url\" is missing, required to process');
            failedParms = true;
        }

        // Validate outputFile
        if (typeof runConfig.outputFile !== 'undefined') {
            console.log('pull0002i - Parameter \"outputFile\" ' + runConfig.outputFile);
        } else {
            console.log('pull0012e - Parameter \"outputFile\" is missing, required to process');
            failedParms = true;
        }

        // Validate fields
        if (typeof runConfig.fields !== 'undefined') {
          var tmp = runConfig.fields
          var hl = tmp.length;
          if (hl === 0 ) {
              console.log('pull0012e - Parameter \"field\" definition located in \"fields\" parameter, required to process');
              failedParms = true;
          } else {
            var p = 0;
            for (var i = 0; i < hl; i++) {
              if (typeof tmp[i].field === 'undefined') {
                p = i + 1;
                console.log('pull0012e - Parameter \"fields\" entry ' + p + ' does not contain \"field\" definition, required to process');
                failedParms = true;
              }
            }
            console.log('pull0002i - Parameter \"fields\" ' + JSON.stringify(runConfig.fields));
          }
        } else {
            console.log('pull0012e - Parameter \"fields\" is missing, required to process');
            failedParms = true;
        }


      } catch (e) {
      inputDB = null;
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
                    console.log('pull0013i - Configuration file: config.json does not exist, no SMS will be configured');
                    deferred.reject('ENOENT');
                }
                if (err.code === 'EACCES') {
                    console.log('pull0014e - Configuration file: config.json has Permission error');
                    deferred.reject('EACCES');
                }
                console.log('pull0015e - Unknown Error reading configuration file: config.json');
                deferred.reject('UNKERR');
            }
            // array of config parms

            try {
                runConfig = JSON.parse(data);
            } catch (e) {
                console.log('pull0016e - Invalid format in config.json configuration file, message: ' + e);
                deferred.reject(e);
                runConfig = '';
            }
            deferred.resolve('OK');
        });

        return deferred.promise;

    } catch (e) {
        console.log('pull0017e - Error reading configuration file: config.json, message: ' + e);
        deferred.reject(e);
    }
}
