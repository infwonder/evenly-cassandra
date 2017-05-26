const Promise = require("bluebird");
const fs = require("fs");
var ec = require('evenly-cassandra');
var ep = require('evenly-cassandra-packer');
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

ec.peers = JSON.parse(fs.readFileSync("./peers.json"));
ec.outdir = '/tmp';
ec.indir = process.argv[2];
ep.triggerTime = 100; // 0.1 secs
ep.maxsize = 1048576; // 1 MB

connect().then( () => 
  { 
    ec.init_watcher_upload(() => {
      ep.watcher(ec.indir, (error, totalsize, filelist) => 
      {
        console.log("New batch of files arrived: totalsize = " + totalsize + ", list: " + JSON.stringify(filelist, null, 2));
        setTimeout(() => { ec.new_qTask_upload(filelist, (err) => { if (err) throw(err); }); }, 2900);
      });
    });

    ec.ULJobQ.on('success', (result, job) => 
    {
      console.log(JSON.stringify(result, null, 2));
      console.log(JSON.stringify(job, null, 2));
    });

    ec.ULJobQ.on('error', (err, job) => { console.log("Queue job error: " + err); ec.client.shutdown(); });
  } 
);


