var MongoClient = require('mongodb').MongoClient;
var url = "mongodb://root:example@localhost:27017/";

async function run() {
	let db = await MongoClient.connect(url, {useUnifiedTopology: true});
	let dbo = db.db("performance");

	await printDuration(findBillingAccounts(dbo, "E959754"));
	await printDuration(findCustomers(dbo, "B113654"));
}

function findBillingAccounts(dbo, ecn) {
	return async () => {
		console.log(`... Finding billing accounts for ECN = ${ecn}`);

		let col = dbo.collection("customers");

		let results = await col.aggregate([
			{ $match: {ecn: ecn} },
			{ $lookup: {from: "customer_policy", localField: "ecn", foreignField: "ecn", as: "customer_policy"} }, // join with customer_policy
			{ $unwind :"$customer_policy" },
			{ $lookup: {from: "policy_billing", localField: "customer_policy.policy", foreignField: "policy", as: "policy_billing"} }, // join with customer_policy
			{ $unwind :"$policy_billing" },
			{ $group: {
				_id: "$ecn",
				billingAccounts: {
					$addToSet: "$policy_billing.billingAccount"
				}
			}}
		]).toArray();

		console.log(JSON.stringify(results, null, 2));
	}
}

function findCustomers(dbo, billingAccount) {
	return async () => {
		console.log(`... Finding customers for billing account ${billingAccount}`);

		let col = dbo.collection("policy_billing");

		let results = await col.aggregate([
			{ $match: {billingAccount: billingAccount} },
			{ $lookup: {from: "customer_policy", localField: "policy", foreignField: "policy", as: "customer_policy"} }, // join with customer_policy
			{ $unwind :"$customer_policy" },
			{ $group: {
				_id: "$billingAccount",
				ecns: {
					$addToSet: "$customer_policy.ecn"
				}
			}}
		]).toArray();

		console.log(JSON.stringify(results, null, 2));
	}
}

async function printDuration(query) {
	let start = Date.now();
	await query();
	let end = Date.now();
	console.log(`Success. Took ${end - start} ms`);
}

(async () => {
	try {
		await run();
		process.exit(0);
	} catch (e) {
		console.error(e);
		process.exit(1);
	}
})();
