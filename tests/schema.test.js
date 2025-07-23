import {expect, test, beforeAll, afterAll, describe} from "bun:test";

import {openMemoryDb} from "./test-util.js";

import {fhirpathToAst} from "../src/fhirpath-parser.js";

import {extractPathsFromAst} from "../src/view-parser.js";
import {pathsToSchema} from "../src/ddb-sql-builder.js"
import fhirSchema from "../schemas/fhir-schema-r4.json";

let db;

beforeAll( done => {
	db = openMemoryDb();
	done();
});

afterAll( done => {
	db.close( () => done());
});


function buildSchemaSubset(fp, resourceType, schema) {
	fp = Array.isArray(fp) ? fp : [fp];
	const asts = fp.map(expr => {
		console.log("FHIRpath Expression: ", expr)
		return fhirpathToAst(expr, resourceType, schema);
	})
	const paths = extractPathsFromAst({asts});
	return pathsToSchema(paths);
}

describe("fhirpath to duckdb sql schemas", () => {

	test("string type", async () => {
		const fp = "id";
		const schema = buildSchemaSubset(fp, "Observation", fhirSchema);
		const target = "{id: 'VARCHAR'}";
		expect(schema.replace(/\s*/g, "")).toEqual(target.replace(/\s*/g, ""));
	});

	test("integer type", async () => {
		const fp = "valueInteger";
		const schema = buildSchemaSubset(fp, "Observation", fhirSchema);
		const target = "{valueInteger: 'INTEGER'}";
		expect(schema.replace(/\s*/g, "")).toEqual(target.replace(/\s*/g, ""));
	});

	test("boolean type", async () => {
		const fp = "valueBoolean";
		const schema = buildSchemaSubset(fp, "Observation", fhirSchema);
		const target = "{valueBoolean: 'BOOLEAN'}";
		expect(schema.replace(/\s*/g, "")).toEqual(target.replace(/\s*/g, ""));
	});

	test("decimal type", async () => {
		const decimalSchema = {"Custom.valueDecimal": {t: "decimal"}}
		const fp = "valueDecimal";
		const schema = buildSchemaSubset(fp, "Custom", decimalSchema);
		const target = "{valueDecimal: 'DOUBLE'}";
		expect(schema.replace(/\s*/g, "")).toEqual(target.replace(/\s*/g, ""));
	});

	test("unknown fhir type should become json", async () => {
		const customSchema = {"Custom": {t: "custom"}}
		const fp = "custom";
		const schema = buildSchemaSubset(fp, "Custom", customSchema);
		const target = "{custom: 'JSON'}";
		expect(schema.replace(/\s*/g, "")).toEqual(target.replace(/\s*/g, ""));
	});

	test("struct", async () => {
		const fp = "valueQuantity.value";
		const schema = buildSchemaSubset(fp, "Observation", fhirSchema);
		const target = "{valueQuantity: 'STRUCT(value DOUBLE)'}";
		expect(schema.replace(/\s*/g, "")).toEqual(target.replace(/\s*/g, ""));	
	})

	test("array", async () => {
		const fp = "name.family";
		const schema = buildSchemaSubset(fp, "Patient", fhirSchema);
		const target = "{name: 'STRUCT(family VARCHAR)[]'}"
		expect(schema.replace(/\s*/g, "")).toEqual(target.replace(/\s*/g, ""));	
	});

	test("array of arrays", async () => {
		const fp = "name.given";
		const schema = buildSchemaSubset(fp, "Patient", fhirSchema);
		const target = "{name: 'STRUCT(given VARCHAR[])[]'}"
		expect(schema.replace(/\s*/g, "")).toEqual(target.replace(/\s*/g, ""));	
	});

	test("multiple paths should be at top level", async () => {
		const fp = ["name.family", "active"]
		const schema = buildSchemaSubset(fp, "Patient", fhirSchema);
		const target = "{name: 'STRUCT(family VARCHAR)[]', active: 'BOOLEAN'}"
		expect(schema.replace(/\s*/g, "")).toEqual(target.replace(/\s*/g, ""));	
	});

	test("overlapping paths should nest", async () => {
		const fp = ["name.family", "name.given"]
		const schema = buildSchemaSubset(fp, "Patient", fhirSchema);
		const target = "{name: 'STRUCT(family VARCHAR, given VARCHAR[])[]'}"
		expect(schema.replace(/\s*/g, "")).toEqual(target.replace(/\s*/g, ""));	
	});

	test("repeated paths should be included once", async () => {
		const fp = ["name.family", "name.family"]
		const schema = buildSchemaSubset(fp, "Patient", fhirSchema);
		const target = "{name: 'STRUCT(family VARCHAR)[]'}"
		expect(schema.replace(/\s*/g, "")).toEqual(target.replace(/\s*/g, ""));
	});

});