const Promise = require("bluebird");
var ec = require('evenly-cassandra');
var connect = Promise.promisify(ec.connect);
var init_storage = Promise.promisify(ec.init_storage);
var wipe_storage = Promise.promisify(ec.wipe_storage);
var join_chunks = Promise.promisify(ec.join_chunks);
var delete_file = Promise.promisify(ec.delete_file);

ec.peers = ['10.100.1.101', '10.100.1.102', '10.100.1.103', '10.100.1.104', '10.100.1.105', '10.100.1.106', '10.100.1.109', '10.100.1.112', '10.100.1.111', '10.100.1.113'];
ec.outdir = '/home/jasonlin/tmp';

connect().then( () =>  
  {
     wipe_storage().then( () => 
        {
           console.log("all records deleted ...");
           ec.client.shutdown(); 
        });
  } 
);

