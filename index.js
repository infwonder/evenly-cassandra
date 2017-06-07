/*
Copyright 2017  University Corporation for Atmospheric Research 

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

'use strict'; 
const fs = require("fs");
const C = require('crypto');
const mkdirp = require('mkdirp');
const cassandra = require("cassandra-driver");
const util = require('util');
const fpath = require('path');
const xrange = require('xrange');
const Promise = require('bluebird');
const queue = require('queue');
const EventEmitter = require("events");
class evenlyEvents extends EventEmitter {}
const evenlySender = new evenlyEvents();

module.exports =
{
  keyspace: "evenly",
  maintable: "storage",
  outdir: undefined,
  indir: undefined,
  peers: [],
  client: undefined,
  maxchunks: 2048, // counts
  maxcsize: 524288,
  maxsize: 67108864,
  slgroup: 3,
  retrial: 20,
  retrial_time: 3500,
  batchsize: 10,
  events: evenlySender,

  _array_groups: function(array, n) {
    if (array.length <= n) return [ array ];

    var p = (array.length-1) % n;
    var ap = array.slice(0, array.length-p);
    var lg = array.slice(array.length-p-1, array.length);
    //console.log(lg);

    var output = ap.map( (i) => 
     { 
       var v = array.indexOf(i);
       var b = array.slice(v-n,v);
       //console.log("v = " + v);
       if (v % n == 0 && b.length != 0) {
         //console.log("DEBUG:" + b);
         return b;
       }
     }).filter( (g) => { return g != undefined; }); 

    if (lg.length != 0) output.push(lg);
    return output;
  },

  _array_ungroup: function(agroup)
  {
    var output = [];
    agroup.map( (g) => { g.map( (i) => { output.push(i); } ); } );

    return output;
  },

  _batch_sessions: function(arraygroups, nextcall, callback)
  {
    // nextcall() takes args of (joblist, ncallback)
    // callback, ncallback, and callback needs to have same args: (err, list)
    // when error, list is "failed list", otherwise list is "done list". 

    var failedlist = []; 
    var donelist = [];
    var current = 0;
    var timeout = 104;

    function _callback(err, ag) 
    {
      if (err) { 
        failedlist.push(ag);
      } else {
        donelist.push(ag);
      }

      if (current == arraygroups.length - 1) {
        if (failedlist.length != 0) {
          return callback("Some jobs were not finished ...", failedlist);
        } else {
          return callback(null, donelist);
        }
      }

      var fsize = 0;
      arraygroups[current].map((i) => 
      {
        var stats = fs.statSync(i);
        fsize += stats.size;
      });

      current++;

      timeout = fsize / 500; 
      if (timeout <= 4000) {
          timeout = 4500;
      } else if (timeout >= 90000) {
          timeout = 95000;
      }
      console.log("DEBUG:!!!!! Waiting " + timeout + " msecs before next job!!!!!");
      
      setTimeout(() => { nextcall(arraygroups[current], (err, list) => { return _callback(err, list); }); }, timeout);
    } 

    //console.log("DEBUG: issuing first call ...");
    nextcall(arraygroups[current], (err, list) => { return _callback(err, list) });
  },

  _cluster_sizes: function cluster_edge(fsize, defcluster)
  {
    var edges = xrange(0, fsize, defcluster).toArray();
    var n_m   = edges.length;
    edges.push(fsize); var o = [];
    xrange(0,n_m).map( (i) => {o.push([edges[i],edges[i+1]-1])} );
    return o;
  },

  connect: function(callback) 
  {
    if (module.exports.client === undefined) 
    {
      var retrypolicy = new cassandra.policies.retry.RetryPolicy;
      var reconnpolicy = new cassandra.policies.reconnection.ConstantReconnectionPolicy;
      var lbpolicy = new cassandra.policies.defaultLoadBalancingPolicy;
      var options = { 
        contactPoints: module.exports.peers, 
             keyspace: module.exports.keyspace,
       promiseFactory: Promise.fromCallback,
        socketOptions: {readTimeout: 50000, connectTimeout: 51000},
             policies: { retry: retrypolicy, reconnection: reconnpolicy, loadBalancing: lbpolicy }
      };
      module.exports.client = new cassandra.Client( options );
    }

    console.log('Connection established...');
    return callback();
  },

  init_storage: function(callback)
  {
    var queryThere = 'select table_name from system_schema.tables where keyspace_name = ?';
    var queryInit  = 'create table ' + module.exports.maintable
          + ' ( filehash ascii, filesize int, chunksize int, chunkcount int, filename text, metadata map<int, ascii>, clustered boolean, PRIMARY KEY (filehash) )';

    if (module.exports.client === undefined) module.exports.connect();

    var client = module.exports.client;

    client.execute(queryThere, [ module.exports.keyspace ])
      .then( (result) =>
      {
         if (result.first() === null) {
           console.log("No table yet, adding ...");
           client.execute(queryInit)
             .then( () => 
             { 
                var slots = xrange(module.exports.maxchunks).map( function(s) { return util.format('slot%d blob', s); } ).join(', ');
                var wait = 0;
                xrange(0,16).map( (i) => {
                   var tid = i.toString(16);
                   var queryTab = "create table sg_" + tid + " (filehash ascii, chunkcount int, " + slots + ", PRIMARY KEY (filehash) )";
                   //console.log("DEBUG: the cql is: " + queryTab + "\n------");
                   setTimeout( () => { client.execute(queryTab, [], {prepare: false})
                     .then( () => { wait = wait + i; if (wait == 120) return callback(); } ); }, 15000);
                });
             })
             .catch( (err) => { return callback(err); } );
         } else {
           console.log("Already initialized. Quit ...");
           return callback();
         }
      });
  },

  wipe_storage: function(callback)
  { 
    if (module.exports.client === undefined) module.exports.connect();
    var client = module.exports.client;
    var k = module.exports.keyspace;
    var s = module.exports.maintable;
    var truncateES = 'truncate table ' + k + '.' + s;
    var wait = 0;

    client.execute(truncateES, [], {prepare: false}).then( (result) => {
      xrange(0,16).map( (i) => 
        {
           var sg = i.toString(16);
           var truncate = 'truncate table ' + k + '.sg_' + sg;
           setTimeout( () => {client.execute(truncate).then( () => { wait = wait + i; if(wait == 120) return callback();});}, 15000);
        });
    })
     .catch( (err) => { client.shutdown; return callback(err);});
  },

  _delete_cluster: function(fhash, metadata, callback)
  {
    var parts = Object.keys(metadata); var wait = 0;

    parts.map( (k) =>
      {
         var fh = metadata[k];
         module.exports.delete_file(fh, (err) => 
             { 
               if (module.exports.client === undefined) module.exports.connect();

               var client = module.exports.client;

               if (err) {
                 client.shutdown(); 
                 return callback(err);
               } 

               var k = module.exports.keyspace;
               var s = module.exports.maintable;
               console.log("deleting part " + fh + " ...");
               wait++; 
               if(wait === parts.length) 
                 {
                    console.log("DEBUG: deleteing top-level cluster meta (hash = " + fhash + ")");
                    var deleteMeta = 'delete from ' + k + '.' + s + ' where filehash = ?';
                    client.execute(deleteMeta, [ fhash ], {prepare: true})
                      .then( () => { return callback(); })
                      .catch( (err) => { return callback(err); } );
                 }
              })
      });
  }, 

  delete_file: function(fhash, callback)
  {
    if (module.exports.client === undefined) module.exports.connect();

    var client = module.exports.client;
    var k = module.exports.keyspace;
    var s = module.exports.maintable;
    var queryExist = 'select filehash, filesize, chunksize, filename, metadata, clustered from ' + k + '.' + s + ' where filehash = ?';

    client.execute(queryExist, [ fhash ], {prepare:true})
      .then( (results) => 
        {
          var there = results.first();
          if (there.filehash !== null) 
          {
             if (there.clustered === true) return module.exports._delete_cluster(fhash, there.metadata, callback);

             var oldmeta = there.metadata;
             var deleteMeta = 'delete from ' + k + '.' + s + ' where filehash = ?';
             var plan = {}; var wait = 0;

             xrange(0,16).map( (h) => { plan[h.toString(16)] = []; } );
             Object.keys(oldmeta).map( (u) => { return plan[oldmeta[u][0]].push(u); } );

             var batchDel = xrange(0,16).map( (i) => {
               var tid = i.toString(16);
               if (plan[tid].length == 0) { return -999; }
               var slots = plan[tid].map( (s) => { return util.format('slot%d', s) }).join(', ');
               var deleteParts = 'delete FROM ' + k + '.sg_' + tid + ' WHERE filehash = ?';
               return { query: deleteParts, params: [ fhash ] };
             }).filter( (j) => { return j != -999} );     // End of xrange

             batchDel.push({query: deleteMeta, params: [fhash] });
             client.batch(batchDel, {prepare: true}, (err) => 
               { 
                  if (err) {
                    return callback(err); 
                  } else {
                    console.log("deletion finished!");
                    return callback(); 
                  }
               });
          }
        }
      ).catch( (err) => { return callback(err); } );  
  },

  _new_cluster: function(path, fsize, defcluster, clcount, callback)
  {
    var fstream = fs.createReadStream(path);
    var fhash   = C.createHash('md5');
    var filemd5;

    console.log("file " + path + " will be stored in " + clcount + " clusters ...");

    fstream.pipe(fhash);
    fhash.on('readable', () => 
    {
      var data = fhash.read();
      if (data) {
        filemd5 = data.toString('hex');
        console.log('Check sum: ' + filemd5);
        var meta = {}; var partsizes = module.exports._cluster_sizes(fsize, defcluster);
        var wait = 0;

        // one-time unique cluster event
        module.exports.events.once(filemd5, (cmeta) => 
        {
          if (module.exports.client === undefined) module.exports.connect();

          var client = module.exports.client;
          var k = module.exports.keyspace;
          var s = module.exports.maintable;
          var queryNew = 'insert INTO ' + k + '.' + s + ' (filehash, filesize, chunksize, chunkcount, filename, metadata, clustered) VALUES (?,?,?,?,?,?,?)';

          if (cmeta === null) {
            client.shutdown();
            return callback("Error: file cluster " + filemd5 + " upload failed!!!!!", filemd5);
          }

          client.execute(queryNew, [filemd5, fsize, defcluster, clcount, path, cmeta, true], {prepare: true})
            .then( () => { Object.keys(cmeta).map( (k) => { fs.unlink(module.exports.outdir + '/_c_' + filemd5 + '_p_' + k); }); callback(null, filemd5); } )
            .catch( (err) => { return callback(err, filemd5); });
        });

        xrange(0, clcount).map( (i) => 
          { 
            var ps = partsizes[i][0];
            var pe = partsizes[i][1];

            console.log("DEBUG: cluster part " + i + ', ps = ' + ps + ', pe = ' + pe);

            var clusterpath = module.exports.outdir + "/_c_" + filemd5 + "_p_" + i;
            var pstream = fs.createReadStream(path, {start: ps, end: pe});
            var ostream = fs.createWriteStream(clusterpath);
            pstream.pipe(ostream);
            ostream.on('close', () => 
              {
                module.exports.new_file(clusterpath, (err, clusterhash) => 
                 {
                   if (err) return callback(err, fhash);
                   meta[i] = clusterhash;  wait++;
                   if (wait === clcount) module.exports.events.emit(filemd5, meta);
                 });
              }); 
          });
      } // end of if (data)
    });
  },

  new_file: function(path, callback)
  {
    var stats = fs.statSync(path);
    var fsize = stats.size;

    if ( fsize > module.exports.maxsize ) 
    {
       var defcluster = 67108864; // fixed size for now...
       var clcount = (fsize - (fsize % defcluster)) / defcluster; 
       if (fsize % defcluster !== 0) clcount++;

       return module.exports._new_cluster(path, fsize, defcluster, clcount, callback);
    }

    // chunk_size should be determined here
    // also calculates how many chunks based on fsize and chunk_size

    var defchunk = module.exports.maxcsize;

    var ccount = (fsize - (fsize % defchunk)) / defchunk; 
    if (fsize % defchunk !== 0) ccount++;

    // Calculating whole file chekcsum for chunk meta
    var fstream = fs.createReadStream(path);
    var fhash   = C.createHash('md5');
    var filemd5;

    fstream.pipe(fhash);

    fhash.on('readable', () => {
      var data = fhash.read();
      if (data) {
        filemd5 = data.toString('hex');
        console.log('Check sum: ' + filemd5);
    
        if (module.exports.client === undefined) module.exports.connect();

        var client = module.exports.client;
        var k = module.exports.keyspace;
        var s = module.exports.maintable;
        var queryNew = 'insert INTO ' + k + '.' + s + ' (filehash, filesize, chunksize, chunkcount, filename, metadata, clustered) VALUES (?,?,?,?,?,?,?)';

        var batchInit = xrange(0,16).map( (i) => {
          var tid = i.toString(16);
          var querySG = 'insert INTO ' + k + '.sg_' + tid + ' (filehash, chunkcount) VALUES (?,?)';
          return {query: querySG, params: [filemd5, ccount]};
        });

        client.batch(batchInit, {prepare: true}, (err) => 
        { 
          if (err) {
            client.shutdown();
            throw(err); 
          }

          module.exports.chunk_file(path, fsize, defchunk, filemd5, ccount);
          module.exports.events.once(filemd5, (meta) => 
            { 
               if (meta === null) {
                 client.shutdown();
                 return callback("Error: file " + filemd5 + " upload failed!!!!!", filemd5);
               }

               client.execute(queryNew, [meta.filehash, meta.size, meta.csize, meta.ccount, meta.filename, meta.piece, false], {prepare: true})
               .then( () => { callback(null, meta.filehash); } )
               .catch( (err) => { return callback(err, meta.filehash); });
            }); 
        });
      }
    });
  },

  chunk_file: function(path, fsize, chunk_size, filemd5, ccount)
  {
    // start building chunks here to make sure it is only done after filemd5 is create!
    // Chopping file into chunks and fit them in protobuf messages
    fs.open(path, 'r', (err, fd) => 
    {
      if (err) throw err;
      var done = 0;

      function _sender(fhash, chash, count, buff, jobs, meta, trial) // assuming the column is first created by new_file().
      {
        if ( trial > module.exports.retrial ) return module.exports.events.emit(fhash, null);
        
        var rmsg = 'sending '; if (trial > 0) rmsg = 'resending (trial: ' + trial + ') ';
        var k = module.exports.keyspace;
        var s = chash[0];
        var querySend = 'update ' + k + '.sg_' + s + ' SET slot' + count + ' = ? WHERE filehash = ?';

        if (module.exports.client === undefined) module.exports.connect();

        var client = module.exports.client;
        client.execute(querySend, [buff, fhash], {prepare: true})
          .then( (result) => { 
              done++; 
              if (done === jobs) module.exports.events.emit(fhash, meta);
              console.log(rmsg + buff.length + " bytes asynchronously for chunk part: No." + count + " out of " + (jobs - 1)); 
          })
          .catch( (err) => 
            {
               trial++; console.log(" ... retry uploading chunk " + count + " (" + trial + "/" + module.exports.retrial + ") ...");
               setTimeout( () => { _sender(fhash, chash, count, buff, jobs, meta, trial); }, module.exports.retrial_time );  
            });
      }
      
      function _readchunk(count, meta) 
      {
        var buff = Buffer.alloc(chunk_size);
    
        fs.read(fd, buff, 0, chunk_size, null, (err, nread, buff) =>
        {
          if (err) throw err;
      
          if (nread === 0)
          {
            // Done reading from fd. Close it.
            fs.close(fd, (err) => { if (err) throw err });

            return;
          }
        
          var data;
        
          if (nread < chunk_size)
          {
            data = buff.slice(0, nread);
          } else {
            data = buff;
          }
        
          var chunksum = C.createHash('md5').update(data).digest('hex');
          //console.log(" part " + count + ", sum: " + chunksum);
          meta.piece[count] = chunksum;
      
          _sender(filemd5, chunksum, count, data, ccount, meta, 0); 
          count = count + 1;
          _readchunk(count, meta);
        });
      }
        
      // Preparing protobuf message for readchunk
      var meta = { filename: path, filehash: filemd5, size: fsize, csize: chunk_size, ccount: ccount, piece: {} };
      _readchunk(0, meta);
        
      console.log("Awaiting results from chunking " + path + "...");

    });
  },
 
  _get_cluster_parts: function(fhash, fmeta, callback)
  {
    var wait = 0;
    var clcount = fmeta.chunkcount;
    var cmeta = fmeta.metadata;
    var name = fpath.basename(fmeta.filename);

    xrange(0, clcount).map( (i) => 
      {
         var chash = cmeta[i];
         module.exports.join_chunks(chash, (err, name) => 
           { 
              if (err) return callback(err, name); 
              wait++; 
              if(wait === clcount) return module.exports._join_clusters(fhash, fmeta, callback); 
           } );
      });
  },

  _join_clusters: function(fhash, clustermeta, callback)
  {
    // stram pipe in order ???
    console.log("DEV: first check manually and see if we get all cluster parts properly...");
    console.log(JSON.stringify(clustermeta, null, 2));

    var total = clustermeta.chunkcount;
    var name = fpath.basename(clustermeta.filename);
    var ts = fs.createWriteStream(module.exports.outdir + '/_b_' + name);

    // recursive, ordered  streams:
    function _join_streams(id) {
      if (id === total) {
          return callback(null, name);
      }
      console.log("DEBUG: Joining part " + (id + 1) + " / " + total);


      var ss = fs.createReadStream(module.exports.outdir + '/_b__c_' + fhash + '_p_' + id);
      if (id === total-1) {
        ss.pipe(ts);
      } else if (id < total) {
        ss.pipe(ts, {end: false});
      }

      ss.on('end', () => { fs.unlinkSync(module.exports.outdir + '/_b__c_' + fhash + '_p_' + id); id++; _join_streams(id);});
    }

    _join_streams(0);
  },
 
  join_chunks: function(fhash, callback) // join from just downloaded chunks with headers... this should be more commonly used.
  {
    if (module.exports.client === undefined) module.exports.connect();

    var missed = {}; xrange(0,16).map( (h) => { missed[h.toString(16)] = []; } );
    var client = module.exports.client;
    var k = module.exports.keyspace;
    var s = module.exports.maintable;
    var queryMeta = 'select filesize, chunksize, chunkcount, filename, metadata, clustered FROM ' + k + '.' + s + ' WHERE filehash = ?';

    client.execute(queryMeta, [ fhash ], {prepare: true})
      .then( (result) => 
        {
          var meta = result.first();

          if (meta.clustered === true) return module.exports._get_cluster_parts(fhash, meta, callback);

          var bsize = meta.filesize;
          var buff  = Buffer.alloc(bsize);
          var name = fpath.basename(meta.filename);
          var totalcount = meta.chunkcount;
          var chunksize = meta.chunksize;
          var mobj = meta.metadata;

          var plan = {}; 
          xrange(0,16).map( (h) => { plan[h.toString(16)] = []; } );
          Object.keys(mobj).map( (u) => { return plan[mobj[u][0]].push(u); } );

          fs.writeFile(module.exports.outdir + '/_b_' + name, buff, (err) => {
            if (err) throw err;

            var fd = fs.openSync(module.exports.outdir + '/_b_' + name, 'r+');
            var actualcount = totalcount - 1;
            var bytewritten = 0; 
            var outcome = 0; var badflag = 0; var misscount = 0;
            //console.log("plan:\n" + JSON.stringify(plan, null, 2));
            xrange(0,16).map( (i) => {
              var tid = i.toString(16);
              if (plan[tid].length == 0) return;
              var slgs = module.exports._array_groups(plan[tid], module.exports.slgroup);
              slgs.map( (gg) => {
                var slots = gg.map( (s) => { return util.format('slot%d', s) }).join(', ');
                var queryParts = 'select ' + slots + ' FROM ' + k + '.sg_' + tid + ' WHERE filehash = ?';
                client.execute(queryParts, [ fhash ], {prepare: true}).then( (result) => 
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
                         if (bytewritten === bsize) { fs.close(fd); return callback(err, name); } 
                       }); // End of fs.write (data)
                    }
                  );
                })
                 .then(() => 
                   { 
                      outcome = outcome + gg.length; 
                      //console.log("good slot: " + outcome);
                      if(outcome >= totalcount && badflag == 1) {
                        fs.close(fd);
                        var robj = { 'bsize': bsize, 'name': name, 'totalcount': totalcount, 'chunksize': chunksize, 'meta': mobj, 'plan': missed, 'mcount': misscount };
                        return setTimeout( () => { module.exports._resume_plan(fhash, robj, 1, callback); }, module.exports.retrial_time);
                      }
                   })
                 .catch( (err) => 
                   { 
                      badflag = 1;
                      outcome = outcome + gg.length; misscount = misscount + gg.length;
                      missed[tid].push(gg); 
                      console.log("bad slot: " + outcome); 
                      if( outcome >= totalcount) {
                        fs.close(fd);
                        var robj = { 'bsize': bsize, 'name': name, 'totalcount': totalcount, 'chunksize': chunksize, 'meta': mobj, 'plan': missed, 'mcount': misscount };
                        return setTimeout( () => { module.exports._resume_plan(fhash, robj, 1, callback); }, module.exports.retrial_time);
                      }
                   });
              });
            });     // End of xrange
          });       // End of fs.writeFile (buff)
        })
      .catch( (err) => { console.log("meta: \n" + err); return callback(err, fhash); });
  },

  _resume_plan: function(fhash, robj, trial, callback)
  {
     if (trial > module.exports.retrial) return callback('file ' + robj.name + " download incomplete ...", fhash); 
     console.log(" ... retry downloading chunks of " + fhash + " (" + trial + "/" + module.exports.retrial + ") ...");
     //console.log("DEBUG: \n" + JSON.stringify(robj.plan, null, 2));

     if (module.exports.client === undefined) module.exports.connect();
     var client = module.exports.client;
     var k = module.exports.keyspace;
     var s = module.exports.maintable;

     var name = robj.name;
     var plan = robj.plan;
     var bsize = robj.bsize;
     var totalcount = robj.totalcount;
     var chunksize = robj.chunksize;
     var mobj = robj.meta;
     var needs = robj.mcount;
//     var lastwritten = robj.written; 
     
     var missed = {}; xrange(0,16).map( (h) => { missed[h.toString(16)] = []; } );
     var fd = fs.openSync(module.exports.outdir + '/_b_' + name, 'r+');
     var actualcount = totalcount - 1;
     var outcome = 0; var badflag = 0; var misscount = 0; 
//     var missedsize = bsize - lastwritten; var bytewritten = 0; 
     var pcounts = 0;
     
     xrange(0,16).map( (i) => {
       var tid = i.toString(16);
       if (plan[tid].length == 0) return;

       var slgs = plan[tid];

       if (needs <= 5) {
         slgs = module.exports._array_groups(module.exports._array_ungroup(plan[tid]), 1);
       } else if (needs > 100) {
         slgs = module.exports._array_groups(module.exports._array_ungroup(plan[tid]), 4);
       }

       slgs.map( (gg) => {
         var slots = gg.map( (s) => { return util.format('slot%d', s) }).join(', ');
         var queryParts = 'select ' + slots + ' FROM ' + k + '.sg_' + tid + ' WHERE filehash = ?';
         client.execute(queryParts, [ fhash ], {prepare: true, consistency: 2}).then( (result) => 
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
                  pcounts++;
                  console.log( written + " bytes written asynchronously for chunk part " + pcounts + " out of " + needs);
                  if (pcounts === needs) { fs.close(fd); return callback(err, name); } 
                }); // End of fs.write (data)
             }
           );
         })
          .then(() => 
            { 
               outcome = outcome + gg.length; 
               //console.log("good slot: " + outcome);
               if(outcome >= needs && badflag == 1) {
                 fs.close(fd); trial++; //var newritten = lastwritten + bytewritten;
                 var robj = { 'bsize': bsize, 'name': name, 'totalcount': totalcount, 'chunksize': chunksize, 'meta': mobj, 'plan': missed, 'mcount': misscount };
                 return setTimeout( () => { module.exports._resume_plan(fhash, robj, trial, callback); }, module.exports.retrial_time);
               }
            })
          .catch( (err) => 
            { 
               badflag = 1;
               outcome = outcome + gg.length; misscount = misscount + gg.length;
               missed[tid].push(gg); 
               console.log("bad slot: " + outcome); 
               if( outcome >= needs) {
                 fs.close(fd); trial++; //var newritten = lastwritten + bytewritten;
                 var robj = { 'bsize': bsize, 'name': name, 'totalcount': totalcount, 'chunksize': chunksize, 'meta': mobj, 'plan': missed, 'mcount': misscount };
                 return setTimeout( () => { module.exports._resume_plan(fhash, robj, trial, callback); }, module.exports.retrial_time);
               }
            });
       });
     });     // End of xrange
  },

  _new_batch_upload: function(filelist, callback)
  {
    var failedlist = []; 
    var plan = module.exports._array_groups(filelist, module.exports.batchsize);
    var wait = 0;

    plan.map( (u) =>
    {
      u.map( (i) => 
      {
        var myfile = i;
        module.exports.new_file(myfile, (err, fhash) => 
        {
          if (err) failedlist.push(myfile);
          wait++;
          if (wait == filelist.length) {
            if (failedlist.length == 0) {
              // if (remove == true) console.log("DEBUG: source files in the list will be removed!");
              return callback(null, filelist);
            } else {
              return callback("Error: some files were not uploaded...", failedlist);
            }
          }
        });
      });
    });
  },

  indir_upload: function(groups, callback) 
  {
    if (module.exports.indir === undefined) throw("Please define indir attribute first ...");

    var fulllist = [];
    var _fulllist = fs.readdirSync(module.exports.indir); // yes, synced ...
    _fulllist.map( (i) => { fulllist.push(module.exports.indir + '/' + i); } );

    var jobgroups = module.exports._array_groups(fulllist, groups);

    module.exports._batch_sessions(jobgroups, module.exports._new_batch_upload, callback);
  },

  _new_batch_download: function(fhashlist, callback)
  {
    var failedlist = []; var donelist = {};
    var plan = module.exports._array_groups(fhashlist, module.exports.batchsize);
    var wait = 0;

    plan.map( (u) => 
    {
      u.map( (i) => 
      {
        var myhash = i;
        module.exports.join_chunks(myhash, (err, name) => 
        {
          if (err) { 
            failedlist.push(myhash); 
          } else {
            donelist[myhash] = name;
          }

          wait++;
          if (wait == fhashlist.length) {
            if (failedlist.length == 0) {
              return callback(null, donelist);
            } else {
              return callback("Error: some files were not downloaded...", failedlist);
            }
          }
        });
      });
    });
  },

  init_watcher_upload: function(callback) 
  {
    if (module.exports.indir === undefined) throw("Please define indir attribute first ...");
    if (module.exports.ULJobQ === undefined) module.exports.ULJobQ = queue(); // initialize queue
    module.exports.ULJobQ.autostart = true;
    module.exports.ULJobQ.concurency = 1;
    module.exports.ULJobQ.timeout = 86400000;
    module.exports.ULJobQ.start((err) => { if (err) throw(err); });

    return callback();
  },

  new_qTask_upload: function(filelist, callback)
  {
    module.exports._q_UL_List.push(filelist);
    module.exports.ULJobQ.push(module.exports._qTask_upload);
    return callback();
  },

  init_watcher_download: function(callback) 
  {
    if (module.exports.outdir === undefined) throw("Please define outdir attribute first ...");
    if (module.exports.DLJobQ === undefined) module.exports.DLJobQ = queue(); // initialize queue
    module.exports.DLJobQ.autostart = true;
    module.exports.ULJobQ.concurency = 18;
    module.exports.ULJobQ.timeout = 86400000;
    module.exports.DLJobQ.start((err) => { if (err) throw(err); });

    return callback();
  },

  new_qTask_download: function(hashlist, callback)
  {
    module.exports._q_DL_List.push(hashlist);
    module.exports.DLJobQ.push(module.exports._qTask_download);
    return callback();
  },

// New form of job queue with queue NPM module ...
  ULJobQ: undefined,
  DLJobQ: undefined,
  _q_UL_List: [],
  _q_DL_List: [],

  _qTask_upload: function(callback)
  {
    if (module.exports._q_UL_List.length === 0) {
      console.log("It seems that the file list is empty ...");
      callback();
    }

    var groups = 4; // fixed for now during tests ...
    var thislist = module.exports._q_UL_List.shift(1);
    var jobgroups = module.exports._array_groups(thislist, groups);

    module.exports._batch_sessions(jobgroups, module.exports._new_batch_upload, callback);
  },

  _qTask_download: function(callback)
  {
    if (module.exports._q_DL_List.length === 0) {
      console.log("It seems that the hash list is empty ...");
      callback();
    }

    var groups = 1; // fixed for now during tests ...
    var thislist = module.exports._q_DL_List.shift(1);
    var jobgroups = module.exports._array_groups(thislist, groups);

    module.exports._batch_sessions(jobgroups, module.exports._new_batch_download, callback);
  },

  _new_batch_download: function(fhashlist, callback)
  {
    var failedlist = []; var donelist = {};
    var plan = module.exports._array_groups(fhashlist, module.exports.batchsize);
    var wait = 0;

    plan.map( (u) => 
    {
      u.map( (i) => 
      {
        var myhash = i;
        module.exports.join_chunks(myhash, (err, name) => 
        {
          if (err) { 
            failedlist.push(myhash); 
          } else {
            donelist[myhash] = name;
          }

          wait++;
          if (wait == fhashlist.length) {
            if (failedlist.length == 0) {
              return callback(null, donelist);
            } else {
              return callback("Error: some files were not downloaded...", failedlist);
            }
          }
        });
      });
    });
  },

  batch_download: function(fhashlist, groups, callback) 
  {
    var jobgroups = module.exports._array_groups(fhashlist, groups);
    
    module.exports._batch_sessions(jobgroups, module.exports._new_batch_download, callback);
  }
  
  // New ToDos:
  // _readiness_probe(hash)
  // _generate_meta(hash)
  // download_by_meta(metapath)
  // download_by_chunk(chunksum, chunk_no, fhash)
};
