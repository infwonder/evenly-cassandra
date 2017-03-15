const ec = require('evenly-cassandra');
const xrange = require('xrange');

/*
var o = ec._cluster_sizes(400,18);
console.log(JSON.stringify(o, null, 2));
*/

function cluster_edge(fsize, defcluster)
{
  var edges = xrange(0, fsize, defcluster).toArray();
  var n_m   = edges.length;
  edges.push(fsize); var o = [];
  xrange(0,n_m).map( (i) => {o.push([edges[i],edges[i+1]])} );
  console.log(JSON.stringify(o, null, 2));
};

cluster_edge(631242752, 104857600);

/*
function _array_ungroup(agroup) 
{
  var output = [];
  agroup.map( (g) => { g.map( (i) => { output.push(i); } ); } );

  return output;
}

var o2 = _array_ungroup(o);
console.log(JSON.stringify(o2, null, 2));
*/

/*
function _array_ungroup(agroup) 
{
  var output = [];
  agroup.map( (g) => { g.map( (i) => { output.push(i); } ); } );

  return output;
}

var a = [[1,2], [], [3,4], [5,6,7], [], [], [8]];
var o = _array_ungroup(a);
console.log(JSON.stringify(o, null, 2));
*/

/*
var a = xrange(0,21).toArray();
var p = a.length % 3; console.log("l = " + a);
var o = a.slice(a.length-p, a.length);
var c = [['a'],['b']]; c.push(o)
console.log(JSON.stringify(c, null, 2));
*/
