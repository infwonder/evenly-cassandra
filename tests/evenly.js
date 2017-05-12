const chai = require("chai");
const expect = chai.expect;
var ei = require("../index.js");

describe("evenly", () => 
{
    it("should have array clustering function", () => 
    {
        var t = ei._array_groups([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25],3);
        var ans = [ [0,1,2],[3,4,5],[6,7,8],[9,10,11],[12,13,14],[15,16,17],[18,19,20],[21,22,23],[24,25] ];

        expect(t).deep.equal(ans);
    });

    it("should have array cluster-edge function (report group start and stopping indeies)", () => 
    {
        var t = ei._cluster_sizes(25,3);
        var ans = [ [0,2],[3,5],[6,8],[9,11],[12,14],[15,17],[18,20],[21,23],[24,24] ];

        expect(t).deep.equal(ans);
    });

    it("should have array regroup function that skips empty groups", () => 
    {
        var a = [[1,2], [], [3,4], [5,6,7], [], [], [8]];
        var t = ei._array_ungroup(a);
        var ans = [1,2,3,4,5,6,7,8];
 
        expect(t).deep.equal(ans);
    });
});
