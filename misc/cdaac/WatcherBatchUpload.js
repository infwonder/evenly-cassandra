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
ec.batchsize = 20;
ep.triggerTime = 2100; // 2.1 secs
ep.maxsize = 671088640; // 640 MB

connect().then( () => 
  { 
    ec.init_watcher_upload(() => {
      ep.watcher(ec.indir, (error, totalsize, filelist) => 
      {
        console.log("New batch of files arrived: \ntotalsize = " + totalsize + ", \nlist: \n" + JSON.stringify(filelist, null, 2));
        ec.new_qTask_upload(filelist, (err) => { if (err) throw(err); });
        if (ec.ULJobQ.length >= 12) { 
            ec.triggerTime = ec.ULJobQ.length * 200; 
            console.log("\n Batch Depth too large, slowing down batch submit to every " + ec.triggerTime + " seconds");
            ep.maxsize = 104857600;
        }
      });
    });

    ec.ULJobQ.on('success', (result, job) => 
    {
      console.log("\n ******** File Batch Uploaded ******** \n" + JSON.stringify(result, null, 2));
      console.log("\n Batch Depth: " + ec.ULJobQ.length);
      if (ec.ULJobQ.length === 0) { 
          ec.triggerTime = 2100; 
          ep.maxsize = 671088640;
      }
    });

    ec.ULJobQ.on('error', (err, job) => { 
        console.log("Queue job error: " + err + " REQUEUING!!!");
        var requeue = ec._array_ungroup(err); 
        ec.new_qTask_upload(requeue, (err) => { if (err) throw(err); });
    });
  } 
);


