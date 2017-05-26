const Promise = require("bluebird");
var ec = require('evenly-cassandra');
var connect = Promise.promisify(ec.connect);

if (process.argv.length <= 2)
{
  console.log("Error: no in file path found");
  process.exit(-1);
}

/*
var inpath = process.argv[2]; // comma seperated list
var inpaths = inpath.split(',');
*/

ec.peers = ['10.100.1.101', '10.100.1.102', '10.100.1.103', '10.100.1.104', '10.100.1.108', '10.100.1.110', '10.100.1.106', '10.100.1.112', '10.100.1.105', '10.100.1.111', '10.100.1.113'];
ec.outdir = '/home/jasonlin/tmp';
ec.indir = process.argv[2];

connect().then( () => 
  {  
/*
    ec._new_batch_upload(inpaths, 10, true, (err, flist) => 
       {
         if (err) {
           console.log("file failed to be uploaded are:" + flist);
           throw(err);
         } 

         console.log("all files have been uploaded");
         ec.client.shutdown(); 
       }); 
*/

    ec.indir_upload(4, (err, aglist) => 
       {
         if (err) {
           console.log("file failed to be uploaded are:" + aglist);
           ec.client.shutdown(); 
           throw(err);
         }

         console.log("all files have been uploaded \n" + JSON.stringify(aglist, null, 2));
         ec.client.shutdown(); 
       });
  } 
);


