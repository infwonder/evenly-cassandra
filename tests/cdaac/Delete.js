const Promise = require("bluebird");
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


ec.peers = ['10.100.1.101', '10.100.1.102', '10.100.1.103', '10.100.1.104', '10.100.1.105', '10.100.1.106', '10.100.1.109', '10.100.1.112', '10.100.1.111', '10.100.1.113'];
ec.outdir = '/home/jasonlin/tmp';

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
