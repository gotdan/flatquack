#!/usr/bin/env bun

import fs from 'fs';
import path from 'path';
import {Glob} from "bun";
import {parseArgs} from "util";
import {templateToQuery} from "./query-builder.js";
import fhirSchema from "../schemas/fhir-schema-r4.json";
import duckdb from "duckdb";

function runQuery(sql) {
	const db = new duckdb.Database(":memory:");
	const startTime = performance.now()
	db.run(sql, (err, result) => {
		if (err) console.warn(err);
		const duration = Math.round(performance.now() - startTime)
		console.log("Completed in " + duration + " ms");
		db.close();
	});
}

function exploreQuery(sql) {
	const db = new duckdb.Database(":memory:");
	db.all(sql, (err, result) => {
		if (err) {
			console.warn(err);
		} else {
			console.log(result)
		}
		db.close();
	});
}


const args = parseArgs({
	args: Bun.argv.slice(2),
	options: {
		"view-path": {
			type: "string", default: ".", 
			required: true, short: "v"
		},
		"view-pattern": {
			type: "string", default: "**/*.vd.json", 
			short: "p"
		},
		"template": {type: "string", short: "t"},
		"schema-file": {type: "string", short: "s"},
		"verbose": {type: "boolean"},
		"mode": {type: "string", short: "m", default: "preview"},
		"param": {type: "string", multiple: true}
	}
});

let templatePath = path.join(import.meta.dir, "../templates/csv.sql");
if (args.values["template"] && args.values["template"][0] == "@") {
	templatePath = path.join(import.meta.dir, "../templates", args.values["template"].slice(1) + ".sql");
} else if (args.values["template"]) {
	templatePath = args.values["template"];
} else  if (!args.values["template"] && args.values["mode"] == "explore") {
	templatePath = path.join(import.meta.dir, "../templates/explore.sql");
}
const template = fs.readFileSync(templatePath, "utf-8");

const params = args.values["param"]
	? args.values["param"].map(v => v.split("="))
	: undefined;

const schema = args.values["schema-file"]
	? JSON.parse(fs.readFileSync(args.values["schema-file"]))
	: fhirSchema;

const glob = new Glob(args.values["view-pattern"]);

for (const file of glob.scanSync(args.values["view-path"],{onlyFiles:true})) {
	const inputPath = path.join(args.values["view-path"], file);
	const basename = path.basename(inputPath, path.extname(inputPath));
	const outputPath = path.join(path.dirname(inputPath), basename + ".sql");

	const view = JSON.parse(fs.readFileSync(inputPath));
	const query = templateToQuery(view, schema, template, params, args.values["verbose"]);

	if (args.values["mode"] == "build") {
		console.log("*** compiling", inputPath, "=>", outputPath, "***");
		fs.writeFileSync(outputPath, query);
	} else if (args.values["mode"] == "run") {
		console.log("*** running", inputPath, "***");
		runQuery(query);
	} else if (args.values["mode"] == "explore") {
		console.log("*** exploring", inputPath, "***");
		exploreQuery(query);
	} else { //preview mode
		console.log("*** compiling", inputPath, "***");
		console.log(query)
	}
}