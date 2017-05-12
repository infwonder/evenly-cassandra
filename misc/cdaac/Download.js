const Promise = require("bluebird");
var ec = require('evenly-cassandra');
var connect = Promise.promisify(ec.connect);
var init_storage = Promise.promisify(ec.init_storage);
var join_chunks = Promise.promisify(ec.join_chunks);
var delete_file = Promise.promisify(ec.delete_file);


if (process.argv.length <= 2)
{
  console.log("Error: missing argument variable ...");
  process.exit(-1);
}

var inpath = process.argv[2];


ec.peers = ['10.100.1.101', '10.100.1.102', '10.100.1.103', '10.100.1.104', '10.100.1.105', '10.100.1.106', '10.100.1.109', '10.100.1.111', '10.100.1.112', '10.100.1.113'];
ec.outdir = '/home/jasonlin/tmp';
//ec.slgroup = 3; 
//ec.retrial = 10;

connect().then( () => 
  {  // here, the "inpath" is actually md5sum of the file... 
         join_chunks(inpath).then( (name) =>
            { 
               console.log("file " + name + " (" + inpath + ") successfully downloaded ...");
               ec.client.shutdown(); 
            })
            .catch( (err) => { console.log(inpath + ": " + JSON.stringify(err, null, 2)); ec.client.shutdown(); process.exit(); });  
  } 
);


