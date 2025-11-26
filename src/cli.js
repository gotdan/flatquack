#!/usr/bin/env bun

import fs from 'fs';
import path from 'path';
import {Glob} from "bun";
import {parseArgs} from "util";
import {templateToQuery} from "./query-builder.js";
import fhirSchema from "../schemas/fhir-schema-r4.json";
import duckdb from "duckdb";
import {format} from "sql-formatter";

// Read package.json for version info
const packageJsonPath = path.join(import.meta.dir, "../package.json");
const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'));

function showHelp() {
	console.log(`
flatquack v${packageJson.version} - FHIR flattening and query generation tool
${packageJson.homepage}

Usage: bunx flatquack [options] (or bun run ./src/cli.js [options] for dev use)

Options:
  -m, --mode <mode>             Execution mode: preview|build|run|explore (default: preview)
  -t, --template <path>         Template file path or @template-name (default: @csv)
  -v, --view-path <path>        Path to search for view definition files (default: ".")
  -p, --view-pattern <pattern>  Glob pattern for view definition files (default: "**/*.vd.json")
  -s, --schema-file <path>      Custom schema file path (default: built-in FHIR R4 schema)
      --macros <path>           Custom macro file or directory (can be repeated)
      --var <name=value>        Values for FHIRPath constants in ViewDefinition (can be repeated)
      --param <name=value>      Template parameters (can be used repeated)
      --verbose                 Enable verbose output
      --help                    Show this help message
      --version                 Show version information

Modes:
  preview   Display the generated SQL query (default)
  build     Write the generated SQL to .sql files
  run       Execute the generated SQL query
  explore   Execute and display first 10 query results

Commonly used built-in Templates:
  @csv          Export flattened data to CSV format (default)
  @parquet      Export flattened data to Parquet format
  @dbt_model    Generate a dbt model (reads from dbt source instead of files)

Examples:
  bunx flatquack
  bunx flatquack --mode build --template @parquet
`);
	process.exit(0);
}

function showVersion() {
	console.log(`flatquack v${packageJson.version}`);
	process.exit(0);
}

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

function loadMacros(macroLocations) {
	if (!macroLocations || macroLocations.length === 0) {
		return null;
	}

	const macroContents = [];

	for (let location of macroLocations) {
		let resolvedPath = location;

		// Handle @-prefixed template macro files
		if (location.startsWith('@')) {
			const macroName = location.slice(1);
			resolvedPath = path.join(import.meta.dir, "../templates", macroName + ".sql");
			
			if (!fs.existsSync(resolvedPath)) {
				console.error(`Error: Template macro file not found: ${macroName} (looked for ${resolvedPath})`);
				process.exit(1);
			}
		}

		const stats = fs.statSync(resolvedPath);

		if (stats.isDirectory()) {
			// Find all .sql files in the directory
			const files = fs.readdirSync(resolvedPath)
				.filter(f => f.endsWith('.sql'))
				.map(f => path.join(resolvedPath, f));

			if (files.length === 0) {
				console.error(`Error: Directory ${resolvedPath} does not contain any .sql files`);
				process.exit(1);
			}

			for (const file of files) {
				const content = fs.readFileSync(file, 'utf-8').trim();
				if (!content.endsWith(';')) {
					console.error(`Error: Macro file ${file} must end with a semicolon`);
					process.exit(1);
				}
				macroContents.push(content);
			}
		} else if (stats.isFile()) {
			const content = fs.readFileSync(resolvedPath, 'utf-8').trim();
			if (!content.endsWith(';')) {
				console.error(`Error: Macro file ${resolvedPath} must end with a semicolon`);
				process.exit(1);
			}
			macroContents.push(content);
		}
	}

	return macroContents.join('\n');
}

function formatSQL(sql) {
	try {
		return format(sql, {
			language: 'duckdb',
			linesBetweenQueries: 0
		});
	} catch (error) {
		// If formatting fails, return the original SQL
		if (args.values["verbose"]) {
			console.warn("Warning: SQL formatting failed, using unformatted SQL:", error.message);
		}
		return sql;
	}
}

const args = parseArgs({
	args: Bun.argv.slice(2),
	options: {
		"view-path": {
			type: "string", default: ".", 
			short: "v"
		},
		"view-pattern": {
			type: "string", default: "**/*.vd.json", 
			short: "p"
		},
		"template": {type: "string", short: "t"},
		"schema-file": {type: "string", short: "s"},
		"macros": {type: "string", multiple: true},
		"verbose": {type: "boolean"},
		"mode": {type: "string", short: "m", default: "preview"},
		"param": {type: "string", multiple: true},
		"var": {type: "string", multiple: true},
		"help": {type: "boolean"},
		"version": {type: "boolean"}
	}
});

// Handle help and version flags
if (args.values["help"]) {
	showHelp();
}

if (args.values["version"]) {
	showVersion();
}

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

const vars = args.values["var"]
	? Object.fromEntries(args.values["var"].map(v => v.split("=")))
	: undefined;

const schema = args.values["schema-file"]
	? JSON.parse(fs.readFileSync(args.values["schema-file"]))
	: fhirSchema;

const customMacros = loadMacros(args.values["macros"]);

const glob = new Glob(args.values["view-pattern"]);

for (const file of glob.scanSync(args.values["view-path"],{onlyFiles:true})) {
	const inputPath = path.join(args.values["view-path"], file);
	const basename = path.basename(inputPath, path.extname(inputPath));
	const outputPath = path.join(path.dirname(inputPath), basename + ".sql");

	const view = JSON.parse(fs.readFileSync(inputPath));
	const query = templateToQuery(view, schema, template, params, args.values["verbose"], undefined, customMacros, vars);
	const formattedQuery = formatSQL(query);

	if (args.values["mode"] == "build") {
		console.log("*** compiling", inputPath, "=>", outputPath, "***");
		fs.writeFileSync(outputPath, formattedQuery);
	} else if (args.values["mode"] == "run") {
		console.log("*** running", inputPath, "***");
		runQuery(formattedQuery);
	} else if (args.values["mode"] == "explore") {
		console.log("*** exploring", inputPath, "***");
		exploreQuery(formattedQuery);
	} else { //preview mode
		console.log("*** compiling", inputPath, "***");
		console.log(formattedQuery)
	}
}