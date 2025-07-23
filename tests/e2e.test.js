import fs from "fs";
import path from "path";
import {expect, test, describe, beforeAll, afterAll} from "bun:test";

import {templateToQuery} from "../src/query-builder.js";
import {testQueryTemplate, openMemoryDb, executeQuery} from "./test-util.js";
import fhirSchema from "../schemas/fhir-schema-r4.json";

let db;
let resourceFile;

const resource = {
	"resourceType": "QuestionnaireResponse",
	"item": [{
		"linkId": "crpValue",
		"definition": "crpValue",
		"text": "CRP value",
		"answer": [{"valueDecimal": 0.0006}]
	}]
};

beforeAll(done => {
	db = openMemoryDb();
	// Create temporary resource file
	resourceFile = path.join(import.meta.dir, "e2e-test-resources.temp.json");
	Bun.write(resourceFile, JSON.stringify([resource]));
	done();
});

afterAll(done => {
	// Clean up temporary file
	if (fs.existsSync(resourceFile)) {
		fs.unlinkSync(resourceFile);
	}
	db.close(() => done());
});

describe("e2e tests", () => {

	test("should not truncate decimal values", async () => {
		const viewDefinition = {
			"resource": "QuestionnaireResponse",
			"select": [{
				"column": [{
					"name": "crpValue",
					"path": "item.where(linkId='crpValue').answer.valueDecimal"
				}]
			}]
		};
		
		const expected = [{"crpValue": 0.0006}];
		const querySql = templateToQuery(
			viewDefinition, fhirSchema, 
			testQueryTemplate, [["test_file_path", resourceFile]], 
			true, true
		);

		const result = await executeQuery(db, querySql);	
		expect(new Set(result)).toEqual(new Set(expected));
	});
});