const Promise = require("bluebird");
const fs = require("fs");
var ec = require('evenly-cassandra');
var connect = Promise.promisify(ec.connect);

if (process.argv.length <= 2)
{
  console.log("Error: no in file path found");
  process.exit(-1);
}


var inpath = process.argv[2]; // comma seperated list
var inpaths = inpath.split(',');

ec.peers = JSON.parse(fs.readFileSync("./peers.json"));
ec.outdir = '/tmp';
//ec.indir = process.argv[2];

connect().then( () => 
  {  
/*
    ec._new_batch_download(inpaths, (err, flist) => 
       {
         if (err) {
           console.log("file failed to be downloaded are:" + flist);
           throw(err);
         } 

         console.log("all files have been downloaded: \n" + JSON.stringify(flist, null, 2));
         ec.client.shutdown(); 
       }); 
*/

    ec.batch_download(inpaths, 2, (err, aglist) => 
       {
         if (err) {
           console.log("file failed to be downloaded are: \n" + aglist);
           ec.client.shutdown(); 
           throw(err);
         }

         console.log("all files have been downloaded: \n" + JSON.stringify(aglist, null, 2));
         ec.client.shutdown(); 
       });

  } 
);


