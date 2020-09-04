var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://root:example@localhost:27017/";

function getRandomInt(min, max) {
	min = Math.ceil(min);
	max = Math.floor(max);
	return Math.floor(Math.random() * (max - min) + min); //The maximum is exclusive and the minimum is inclusive
}

async function insertOne(col, obj) {
	return new Promise(function (ok, fail) {
		col.insertOne(obj, function (err, data) {
			if (err) {
				return fail(err);
			}
			ok(data);
		});
	});
}

async function run() {

	let db = await MongoClient.connect(url);
	let dbo = db.db("performance");
	let col = dbo.collection("customer_policy");
	
	let tasks = [];
	let i;
	let start = Date.now();

	for (i=1; i<=1000000; i++) {

		// cusomer object
		var obj = {
			ecn: `E${i}`,
			last_upd: new Date(`2020-08-1${i%10}`).toISOString()
		};

		// each customer will have between 1 and 5 policies
		let numOfPolicies = getRandomInt(1, 5);

		for(let polIdx = 0; polIdx <numOfPolicies; polIdx++) {
			obj["_id"] = null;
			obj["policy"] = `P${(numOfPolicies * i * (polIdx+1)) % 1000001}`; // policy id will be between 1 and 1M
			tasks.push(insertOne(col, obj));
		}


		if (i%1000 === 0) {
			await Promise.all(tasks);
			console.info(`Processed ${i}`);
			tasks = [];
		}
	}

	let end = Date.now();
	console.log(`Success. Took ${end - start} ms`);
	process.exit(0);
}

(async () => {
	try {
		await run();
	} catch (e) {
		console.error(e);
		process.exit(1);
	}
})();
