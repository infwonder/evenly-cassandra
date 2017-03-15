const fs = require("fs");
const C = require('crypto');
const util = require('util');
const fpath = require('path');
const xrange = require('xrange');
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

var fhash = process.argv[2];


ec.peers = ['10.100.1.101', '10.100.1.102', '10.100.1.103', '10.100.1.104', '10.100.1.105', '10.100.1.106', '10.100.1.108', '10.100.1.109', '10.100.1.110', '10.100.1.111', '10.100.1.113'];
ec.outdir = '/home/jasonlin/tmp';

connect().then( () => 
  {  
     var client = ec.client;
     var k = ec.keyspace;
     var s = ec.maintable;
     var queryMeta = 'select filesize, chunksize, chunkcount, filename, metadata FROM ' + k + '.' + s + ' WHERE filehash = ?';

    client.execute(queryMeta, [ fhash ], {prepare: true, connectTimeout:61000, readTimeout: 60000})
      .then( (result) => 
        {
          var meta = result.first();
          var bsize = meta.filesize;
          var buff  = Buffer.alloc(bsize);
          var name = fpath.basename(meta.filename);
          var totalcount = meta.chunkcount;
          var chunksize = meta.chunksize;
          var mobj = meta.metadata;

          var plan = {}; 
          xrange(0,16).map( (h) => { plan[h.toString(16)] = []; } );
          Object.keys(mobj).map( (u) => { return plan[mobj[u][0]].push(u); } );

          fs.writeFile(ec.outdir + '/_b_' + name, buff, (err) => {
            if (err) throw err;

            var fd = fs.openSync(ec.outdir + '/_b_' + name, 'r+');
            var actualcount = totalcount - 1;
            var bytewritten = 0;
            //console.log("plan:\n" + JSON.stringify(plan, null, 2));

            xrange(0,16).map( (i) => {
              var tid = i.toString(16);
              if (plan[tid].length == 0) return;
              var slgs = ec._array_groups(plan[tid], ec.slgroup);
              slgs.map( (gg) => {
                var slots = gg.map( (s) => { return util.format('slot%d', s) }).join(', ');
                var queryParts = 'select ' + slots + ' FROM ' + k + '.sg_' + tid + ' WHERE filehash = ?';
                client.execute(queryParts, [ fhash ], {prepare: true, connectTimeout:61000, readTimeout: 60000}).then( (result) => 
                {
                  var output = result.first();
                  gg.map( (p) => 
                    {
                       var offset = p * chunksize;
                       var chunk = output['slot' + p];
                       var csize = chunk.length;
                       var chunksum = C.createHash('md5').update(chunk).digest('hex');
                       if (mobj[p] !== chunksum) throw("chunk "+ p +" checksum mismatch!!!!!");
                       fs.write(fd, chunk, 0, csize, offset, (err, written) => {
                         if (err) throw err;
                         bytewritten = bytewritten + written;
                         console.log( bytewritten + "/" + bsize + " bytes written (" + written + ") asynchronously for chunk part " + p + ' out of ' + actualcount);
                         if (bytewritten === bsize) { fs.close(fd); return callback(); } 
                       }); // End of fs.write (data)
                    }
                  );
                }).catch( (err) => { throw(err) } );
              });
            });     // End of xrange
          });       // End of fs.writeFile (buff)
        })
      .catch( (err) => { console.log(err); client.shutdown(); });
  } 
);


