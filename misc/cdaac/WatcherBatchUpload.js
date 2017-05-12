const Promise = require("bluebird");
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

ec.peers = ['10.100.1.101', '10.100.1.102', '10.100.1.103', '10.100.1.104', '10.100.1.108', '10.100.1.110', '10.100.1.106', '10.100.1.112', '10.100.1.105', '10.100.1.111', '10.100.1.113'];
ec.outdir = '/home/jasonlin/tmp';
ec.indir = process.argv[2];
ep.triggerTime = 15000; // 15 secs
ep.maxsize = 314572800; // 300 MB;

connect().then( () => 
  { 
    ec.init_watcher_upload(() => {
      ep.watcher(ec.indir, (error, totalsize, filelist) => 
      {
        console.log("New batch of files arrived: totalsize = " + totalsize + ", list: " + JSON.stringify(filelist, null, 2));
        ec.new_qTask_upload(filelist, (err) => { if (err) throw(err); });
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


