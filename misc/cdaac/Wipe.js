const Promise = require("bluebird");
const fs = require("fs");
var ec = require('evenly-cassandra');
var connect = Promise.promisify(ec.connect);
var init_storage = Promise.promisify(ec.init_storage);
var wipe_storage = Promise.promisify(ec.wipe_storage);
var join_chunks = Promise.promisify(ec.join_chunks);
var delete_file = Promise.promisify(ec.delete_file);

ec.peers = JSON.parse(fs.readFileSync("./peers.json"));
ec.outdir = '/tmp';

connect().then( () =>  
  {
     wipe_storage().then( () => 
        {
           console.log("all records deleted ...");
           ec.client.shutdown(); 
        });
  } 
);

