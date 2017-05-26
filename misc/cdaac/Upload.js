const Promise = require("bluebird");
const fs = require("fs");
var ec = require('evenly-cassandra');
var connect = Promise.promisify(ec.connect);
var init_storage = Promise.promisify(ec.init_storage);
var join_chunks = Promise.promisify(ec.join_chunks);
var delete_file = Promise.promisify(ec.delete_file);


if (process.argv.length <= 2)
{
  console.log("Error: no in file path found");
  process.exit(-1);
}

var inpath = process.argv[2];


ec.peers = JSON.parse(fs.readFileSync("./peers.json"));
ec.outdir = '/tmp';

connect().then( () => 
  {  // here, inpath is fullpath to file you want to upload.
    ec.new_file(inpath, (err, hash) => 
       {
         if (err) {
           console.log("file " + inpath + " failed to be uploaded ...");
           ec.client.shutdown(); 
           throw(err);
         } 

         console.log("file " + hash + " has been uploaded");
         ec.client.shutdown(); 
       }); 
  } 
);


