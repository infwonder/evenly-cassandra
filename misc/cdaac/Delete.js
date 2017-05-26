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
  {
     // here, the "inpath" is actually md5sum of the file... 
     delete_file(inpath).then( () => 
        {
           console.log("All records of " + inpath + " were deleted ...");
           ec.client.shutdown(); 
        });
  } 
);
