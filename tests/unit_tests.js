var TEST_BUCKET = "basho_test";
var MR_TEST_BUCKET = 'mr_test';
var SIBLING_BUCKET = 'basho_sibs';

var currentClient = null;

/** Start helpers **/
function findLink(object, tag, link) {
  var found = false;
  var links = object.getLinks();
  for (var i = 0; i < links.length; i++) {
    if (links[i].tag === tag && links[i].target === link) {
      found = true;
      break;
    }
  }
  return found;
};

function setupClient() {
  currentClient = new RiakClient();
};

function setupBucket(test) {
  setupClient();
  currentClient.bucket(TEST_BUCKET, test);
}

function setupSiblingBucket(test) {
  setupClient();
  currentClient.bucket(SIBLING_BUCKET, test);
}

function setupObject(createIfMissing, test) {
  setupClient();
  if (createIfMissing === true) {
    setupBucket(function(bucket, req) {
		  bucket.get_or_new('td1', test); } );
  }
  else {
    setupBucket(function(bucket, req) {
		bucket.get('td1', test); } );
  }
}

function storeObject(test) {
  setupClient();
  setupObject(true, function(status, obj, req) {
		obj.store(test); } );
}

/** End helpers **/

function lookupMissingObject() {
  stop();
  setupObject(false, function(status, object, req) {
		ok(status === 'failed', 'Failed status for missing object');
		ok(object == null, "Nonexistent object returns null");
		start(); } );
};

function createMissingObject() {
  stop();
  setupObject(true, function(status, object, req) {
		ok(status === 'ok', "'ok' status for object creation");
		ok(object, "Nonexistent object created");
		start(); } );
};

function storeMissingObject() {
  stop();
  setupObject(true, function(status, object, req) {
		object.body = "testing";
		object.contentType = "text/plain";
		object.store(function(status, object, req) {
			       ok(status === 'ok', "'ok' status for object save");
			       ok(object !== null, "Object saved");
			       ok(object.vclock !== null &&
				  object.vclock.length > 0, "Object vclock set");
			       start(); } ); } );
};

function updateObject() {
  stop();
  setupBucket(function(bucket, req) {
		bucket.store(function(nb, req) {
			       nb.get_or_new('td1', function(status, obj, req) {
					       ok(obj !== null, "Object saved");
					       ok(obj.vclock !== null &&
						  obj.vclock.length > 0, "Object vclock set");
					       obj.body = "Testing";
					       obj.contentType = "text/plain";
					       obj.store(function(status, newObj, req) {
							   ok(newObj != null, "Object updated");
							   ok(newObj.vclock !== obj.vclock, "Vclock changed");
							   start(); } ); } ); } ); } );
};

function deleteObject() {
  stop();
  setupObject(false, function(status, obj, req) {
		equals(status, "ok", "Object retrieved");
		obj.remove(function(flag, req) {
			     equals(flag, true, "Object deleted");
			     start(); } ); } );
};

function resolveSiblings() {
  stop();
  setupSiblingBucket(function(bucket, req) {
		bucket.allowsMultiples(true);
		bucket.store(function(newBucket, req) {
			       newBucket.get_or_new('td10', function(status, object, req) {
						      ok(status === 'ok', "'ok' status for object creation");
						      object.contentType = 'text/plain';
						      object.body = 'Hello';
						      object.store(function(status, newObject, req) {
								     ok(status === 'ok', "'ok' status for object store");
								     object.vclock = null;
								     object.body = 'Goodbye';
								     object.store(function(status, siblings, req) {
										    ok(status === 'siblings', "Status reflects sibling creation");
										    equals(siblings.length, 2, "2 siblings found");
										    siblings[0].store(function(status, finalObj, req) {
													ok(status === 'ok', 'Final sibling stored');
													equals(finalObj.body, siblings[0].body, "Correct sibling stored");
													finalObj.client.bucket(TEST_BUCKET, function(bucket, req) {
																 bucket.allowsMultiples(false);
																 bucket.store();
																 start(); } ); } ); } ); } ); } ); } ); } );
};

function storeLink() {
  stop();
  var link = '/riak/' + TEST_BUCKET + '/td1';
  storeObject(function(status, object, req) {
		object.store(function(status, object, req) {
			       ok(object !== null, "Object saved");
			       ok(object.links.length == 1, "New object has 1 link");
			       object.addLink(link, "testlink");
			       object.store(function(status, newObj, req) {
					      ok(newObj !== null, "Object saved");
					      ok(newObj.vclock !== object.vclock, "Vclock changed");
					      ok(newObj.getLinks().length == 2, "Link was saved");
					      ok(findLink(newObj, "testlink", link), "Link was found in collection");
					      start(); } ); } ); } );
};

function deleteLink() {
  stop();
  setupObject(false, function(status, obj, req) {
		obj.clearLinks();
		obj.store(function(status, n, req) {
			    ok(n !== null, "Object saved");
			    equals(n.links.length, 1, "Links collection reset");
			    start(); } ); } );
};

function readNValue() {
  stop();
  setupBucket(function(bucket, req) {
		ok(bucket.nValue() > 0, "N-Value is set");
		start(); } );
}

function readAllowMult() {
  stop();
  setupBucket(function(bucket, req) {
		equals(bucket.allowsMultiples(), false, "allowsMultiples defaults to false");
		start(); } );
}

function updateNValue() {
  stop();
  setupBucket(function(bucket, req) {
		var old = bucket.nValue();
		var newValue = old + 1;
		bucket.nValue(newValue);
		bucket.store(function(nb, req) {
			       equals(nb.nValue(), newValue, "N-Value updated");
		               nb.nValue(old);
		               nb.store(function(b, req) {
					  equals(b.nValue(), old);
					  start(); } ); } ); } );
}

function updateAllowMult() {
  stop();
  setupBucket(function(bucket, req) {
		var old = bucket.allowsMultiples();
		var newValue = !old;
		bucket.allowsMultiples(newValue);
		bucket.store(function(nb, req) {
			       equals(nb.allowsMultiples(), newValue, "allowsMultiples updated");
			       nb.allowsMultiples(false);
			       nb.store(function(b, req) {
					  equals(b.allowsMultiples(), false, "allowsMultiples reset to default");
					  start(); } ); } ); } );
}

function bucketMap() {
  stop();
  setupClient();
  currentClient.bucket(MR_TEST_BUCKET, function(bucket, req) {
			   bucket.map({language: 'javascript',
				       name: 'Riak.mapValuesJson',
				       keep: true}).run(function(flag, results, req) {
							  ok(flag, 'Job ran successfully');
							  ok(results !== null, 'Map results not null');
							  equals(results.length, 3, 'Map results contain data');
							  start(); } ); } );
}

function bucketMapReduce() {
  stop();
  setupClient();
  currentClient.bucket(MR_TEST_BUCKET, function(bucket, req) {
			 bucket.map({language: 'javascript',
				     name: 'Riak.mapValuesJson',
				     keep: false}).reduce({language: 'javascript',
							   name: 'Riak.reduceSum',
							   keep: true}).run(function(flag, results, req) {
									      ok(flag, 'Job ran successfully');
									      ok(results !== null, 'Reduce results not null');
									      equals(results.length, 1, 'Map/Reduce results contain data');
									      equals(results[0], 3, 'Map/Reduce summed correctly');
									      start(); } ); } );
}

function objectMap() {
  stop();
  setupClient();
  currentClient.bucket(MR_TEST_BUCKET, function(bucket, req) {
			 bucket.get('first', function(status, obj, req) {
				      ok(obj, 'M/R test object found');
				      obj.map({language: 'javascript',
					       source: function(obj)  { return Riak.mapValuesJson(obj); },
					       keep: true}).run(function(flag, results, req) {
								  ok(flag, 'Job ran successfully');
								  ok(results !== null, 'Map results not null');
								  equals(results.length, 1, 'Map results contain data');
								  equals(results[0], 1, 'Map results are correct');
								  start(); } ); } ); } );
}

function objectMapReduce() {
  stop();
  setupClient();
  currentClient.bucket(MR_TEST_BUCKET, function(bucket, req) {
			 bucket.get('first', function(status, obj, req) {
				      ok(obj, 'M/R test object found');
				      obj.map({language: 'javascript',
					       source: function(obj)  { return Riak.mapValuesJson(obj); },
					       keep: false}).reduce({language: 'javascript',
								     source: function(value, arg) { return [value[0] * 5]; },
								     keep: true}).run(function(flag, results, req) {
											ok(flag, 'Job ran successfully');
											ok(results !== null, 'Map results not null');
											equals(results.length, 1, 'Map results contain data');
											equals(results[0], 5, 'Reduce results are correct');
											start(); } ); } ); } );
}

function bucketLinkWalk() {
  stop();
  setupClient();
  currentClient.bucket(MR_TEST_BUCKET, function(bucket, req) {
			 bucket.link({tag: 'test'}).run(function(flag, results, req) {
							  ok(flag, 'Job ran successfully');
							  ok(results !== null, 'Map results not null');
							  equals(results.length, 2, 'Link results are correct');
							  start(); } ); } );
}

function badMap() {
  stop();
  setupClient();
  currentClient.bucket(MR_TEST_BUCKET, function(bucket, req) {
			   bucket.map({language: 'javascript',
				       source: function(value) { return [JSON.parse(value)]; },
				       keep: true}).run(function(flag, results, req) {
							  ok(!flag, 'Job failed');
							  ok(results.error !== null, 'Error returned from server');
							  ok(results.error.lineno > 0, 'Line number given');
							  ok(results.error.message.length > 0, 'Error message given');
							  start(); } ); } );

}

function runTests() {
  module("Storage");
  test("Create", 2, createMissingObject);
  test("Store", 3, storeMissingObject);
  test("Create", 2, createMissingObject);
  test("Update", 4, updateObject);
  test("Siblings", resolveSiblings);
  test("Delete", 2, deleteObject);

  module("Links");
  test("Create", 2, createMissingObject);
  test("Update", 6, storeLink);
  test("Delete", 2, deleteLink);

  module("Bucket Properties");
  test("N-Value", 1, readNValue);
  test("Update N-Value", 2, updateNValue);
  test("Allow Multiples", 1, readAllowMult);
  test("Update Allow Multiples", 2, updateAllowMult);

  module("Map/Reduce");
  test("Bucket-level map", 3, bucketMap);
  test("Bucket-level map/reduce", 4, bucketMapReduce);
  test("Object-level map", 5, objectMap);
  test("Object-level map/reduce", 5, objectMapReduce);
  test("Bad Map job", 4, badMap);

  module("Link walking");
  test("Bucket-level link walk", 3, bucketLinkWalk);
}

$(document).ready(runTests);
