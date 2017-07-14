# datapull
<br>

A node.js application that pulls data from an IBM Cloudant or Apache CouchDB database.  The application requires a configuration file: config.json that contains the parameters to control the pulling of data and output.  This application is run from a terminal / command prompt or can be invoked from a batch script. 

Application features include:

- Pull data from an existing database
 
- Output data in a CSV formatted file

- Select the database fields that are to be pulled

Installation
================================================================================

<b>

		Node.js and NPM are required to install and execute this application
</b>

Download the source files and place in a directory.  Change to the install directory where the files were placed. Run the following NPM command to install the require Node modules:

	npm install

Once the above has successfully completed the application can be run.  The configuration file provided in this install does not conatin a valid database definition yet the program execute an indicated that zero records where pulled as shown by the following console message.

<b>
pull4001i - DB Record count: 0</b>
<br>
<br>

<b>Note:</b> If the program is started without defining the configuration file parameter it will default to looking in the application directory for a configuration file named 'config.json'.

Configuration file
================================================================================

The configuration file defines the required processing parameters needed to pull the data.  The file is formatted using JSON (JavaScript Object Notation).  Below is an example of a configuration file defined with the <b>minimum</b> set of parameters:

    {
    "inputDB":
      {
        "dbname" : "tweets",
        "username": "",
        "password": "",
        "host": "",
        "port": 443,
        "url":"https://bluemix.cloudant.com"
      },

      "fields": [
        {"field": "payload.tweet"},  
        {"field": "payload.score"},
        {"field": "payload.location"}
      ],

      "outputFile" : "data.csv"
    }

<b>Note</b> the use of double quotes, curly brackets, colons, commas, etc. in the definition of the parameter values. These are required to create a properly formatted JSON file.  Additional information regarding valid JSON can be obtained at: <http://www.json.org>

<br>

## Parameters

The key parameter and sub-parameters are case sensitive sensitive.  The following tables lists the parameter, required or optional, valid values, default value, description, sub-parameters, and example(s).  
<br>

### "inputDB"

| Req/Opt | Valid Value(s) | Default | Description |
| :----:  | -----          | :----:  | -----       |
| REQ | sub-params  | N/A | Required parameter for the input database.  This parameter contains required sub-parameters.


Sub-parameters:


| Sub-Parameter | Req/Opt | Valid Value(s) | Default | Description |
| :-------: | :----:  | -----        | :----:  | -----       | 
| "dbname" | Req | string | N/A | Name of the database. 
| "url" | Req | string | none | Url where the database is located. 



Example: 
<br>
1 - Create new instance of a database named 'doctors' using Apache CouchDB installed locally. 
<br>
<b>

    "inputDB":  
     { 
        "dbname"  : "tweets",
        "url"     : "http://localhost:5984"
     }
</b>

2 - Insert into an existing database named 'costs' using IBM Cloudant running in IBM Bluemix.
<b>
<br>


    "inputDB":  
     { 
        "dbname"  : "tweets",
        "url"     : "https://20c66bd5-459f-dc77-dc77-a286897f74ec-bluemix:6a0864d88250269e2988b43391fa6b054c5ab3688ef8b58c7668a6cf32cc6611@20c66bd5-dc77-459f-a24d-a286897f74ec-bluemix.cloudant.com"

     }

</b>



<br>

### "outputFile"

| Req/Opt | Valid Value(s) | Default | Description |
| :----:  | -----          | :----:  | -----       |
| REQ | file name | none | Defines the name of the output file where the CSV data will be written. 



Example: 
<br>
1 - Output to fully qualified path
<br>
<b>

    "outputfile": "/Users/daveweilert/data_out.txt"

</b>

<br>
2 - Output to directory where program is executed from.
<br>
<b>

    "outputfile": "data_out.txt"

</b>

#### Windows file output

If using Windows operating system the output file name can use  the drive letter and <b>must</b> use the forward slash syntax:

Example:  "filename"    : "D:/Users/daveweilert/data_out.txt"

Optionally for Windows the use the './' or '~' syntax are permitted instead of the drive letter syntax. 

<br>
<br>

### "fields"

| Req/Opt | Valid Value(s) | Default | Description |
| :----:  | -----          | :----:  | -----       |
| REQ | sub-parms | N/A | Defines the input fields from the database that are to be pulled. 

Sub-parameters:


| Sub-Parameter | Req/Opt | Valid Value(s) | Default | Description |
| :-------: | :----:  | -----        | :----:  | -----       |
| "filed" | REQ | N/A | string | Value that defines the full JSON path the data 

Sample document:

    { "firstname": "Dave",
       "lastname": "Weilert",
       "description" :
          {
            "eyecolor": "blue",
            "haircolor": "brown"
          }
    
    }


Example: 
<br>
1 - Pull data fields from document.
<br>
<b>

    "fields": 
      {
        "field"    : "firstname",
        "field"    : "lastname"
        "field"    : "description.eyecolor",
        "field"    : "description.haircolor"
      }

</b>



Running datapull
================================================================================

### Processing output log

As the application is executing processing messages are written to the console.  

### Run time parameters at start

The node.js application is delivered to use the npm command to execute the dataloader application.  Examples of starting the program with or without the configuration file parameter follow:

  npm start    (without a configuration file will use the default config.json file located in the directory where this command was executed)

	Using three parameters:

	npm start ./config_4.json

The third parameter is the configuration file name.  The parameter can be defined with the complete path to the configuration file or use 'from' , 'home', or  'drive letter' syntax:

#### from example
npm start ./example/config.json  (will look for the file from the current path)

#### home example
npm start ~/example/config.json  (will look for the file from the user home path)

#### drive letter example  (Windows only operating system support)
npm start C:\Users\daveweilert\files\config.json  



Example console output: 
================================================================================
pull0000i - Looking for configuration file
pull0002i - Parameter "inputDB" located
pull0002i - Parameter "inputDB.dbname" tweets
pull0002i - Parameter "inputDB.url" https://bluemix.cloudant.com
pull0002i - Parameter "outputFile" data.csv
pull0002i - Parameter "fields" [{"field":"payload.tweet"},{"field":"payload.score"},{"field":"payload.location"}]
pull0012i - Initialized DB: tweets
pull4000i - Retrieving data from database
pull4001i - DB Record count: 41441
pull0020i - Output to CSV file: data.csv  -  fields: payload.tweet payload.score payload.location
pull00221 - Output complete 





Maintainer
================================================================================

 Dave Weilert

http://github.com/daveweilert/datapull.git/

<b>datapull</b> was created as the result of needing a tool to extract data quickly and easily into IBM Cloudant and Apache CouchDB and place the data in a CSV file.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
