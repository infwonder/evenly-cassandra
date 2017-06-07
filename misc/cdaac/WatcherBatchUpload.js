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
ep.triggerTime = 2100; // 2.1 secs
ep.maxsize = 671088640; // 640 MB

connect().then( () => 
  { 
    ec.init_watcher_upload(() => {
      ep.watcher(ec.indir, (error, totalsize, filelist) => 
      {
        console.log("New batch of files arrived: \ntotalsize = " + totalsize + ", \nlist: \n" + JSON.stringify(filelist, null, 2));
        ec.new_qTask_upload(filelist, (err) => { if (err) throw(err); });
      });
    });

    ec.ULJobQ.on('success', (result, job) => 
    {
      console.log("\n ******** File Batch Uploaded ******** \n" + JSON.stringify(result, null, 2));
      console.log("\n Batch Depth: " + ec.ULJobQ.length);
    });

    ec.ULJobQ.on('error', (err, job) => { console.log("Queue job error: " + err); ec.client.shutdown(); });
  } 
);


