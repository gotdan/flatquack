import {expect, test, beforeAll, afterAll, describe} from "bun:test";
import path from "path";

import {openMemoryDb} from "./test-util.js";

import {fhirpathToAst} from "../src/fhirpath-parser.js";
import {astToSql} from "../src/ddb-sql-builder.js"
import fhirSchema from "../schemas/fhir-schema-r4.json";

let db;

beforeAll( done => {
	db = openMemoryDb();
	done();
});

afterAll( done => {
	db.close( () => done());
});

function testQuery(querySegment, resource, duckSchema) {
	console.log("DuckDB Query: ", querySegment)
	const filePath = path.join(import.meta.dirname, "./data.temp.json");
	Bun.write(filePath, JSON.stringify([resource]));
	const query = duckSchema 
		? `SELECT ${querySegment} AS result FROM read_json('${filePath}', columns=${duckSchema})`
		: `SELECT ${querySegment} AS result FROM read_json_auto('${filePath}')`;
	return new Promise( (resolve, reject) => {
		db.all(query, (err, res) => {
			if (err) return reject(err);
			resolve(res[0].result);
		})
	})
}

function buildQuery(fp, resourceType, schema) {
	console.log("FHIRpath Expression: ", fp)
	const fpAst = fhirpathToAst(fp, resourceType, schema);
	return astToSql(fpAst).sql;
}

const simplePatient = {
	resourceType: "Patient", 
	id: "id-123",
	name: [{family: "f1"}],
	link: [{
        other: {reference: "Patient/456"}
    }]
};

const simpleObservation = {
	resourceType: "Observation", 
	id: "123",
	subject: {reference: "Patient/456"},
	code: {
		coding: [{
			system: 's1', code: 'c1'
		}]
	},
	valueString: "123"
};

const nullFamilyName = {
	resourceType: "Patient",
	id: "123",
}

const multipleNames = {
	resourceType: "Patient",
	id: "123",
	name: [{
		use: "official",
		family: "f1"
	},{ 
		use: "nickname",
		family: "f2", 
		given: ["g1", "g2"]
	}]
};

const unionPatient = {
	resourceType: "Patient",
	id: "123",
	address: [{postalCode: "z1"}],
	contact: [{
		address: {postalCode: "z2"}
	}]
}

const contactOnlyPatient = {
	resourceType: "Patient",
	id: "pt-missing-telecom",
	contact: [{
		telecom: [{
			value: "c1",
			system: "sms"
		}]
	}]

}

describe("custom fhirpath features to duckdb sql", () => {

	test("multiple columns", async () => {
		const fp = "name._forEach(_col('use', use), _col('last', family))";
		const resource = multipleNames;
		const target = [{use: "official", last: "f1"}, {use: "nickname", last: "f2"}];
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("initial _forEach function", async () => {
		const fp = "_forEach(_col('id', id), _col('last', name.family))";
		const resource = simplePatient;
		const target = {id: "id-123", last: "f1"}
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("_forEach with join", async () => {
		const fp = "name._forEach(_col('given', given.join()))";
		const resource = multipleNames;
		const target = "g1g2";
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result[1].given).toEqual(target);
	})

	test("null value in _forEachOrNull", async () => {
		const fp = "_forEach(_col('id',id), _col('pt_name',name._forEach(_col('family', family))))";
		const resource = nullFamilyName;
		const target = {id: "123", pt_name: null};
		const duckSchema = "{id: 'VARCHAR', name: 'STRUCT(family VARCHAR)[]'}";
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource, duckSchema);
		expect(result).toEqual(target);
	})

	test("initial _unionAll function", async () => {
		const fp = "_unionAll(address.postalCode, contact.address.postalCode)";
		const resource = unionPatient;
		const target = ["z1", "z2"];
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("nested _unionAll function", async () => {
		const fp = "_forEach(_col('id',id), _col('zip', _unionAll(address.postalCode, contact.address.postalCode)))";
		const resource = unionPatient;
		const target = {id: "123", zip: ["z1", "z2"]};
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("_unionAll preserves right-hand results when left side is null", async () => {
		const fp = "_forEach(_col('id', id), _col_collection('unioned', _unionAll(telecom._forEach(_col('tel', value), _col('sys', system)), contact.telecom._forEach(_col('tel', value), _col('sys', system)))))";
		const resource = contactOnlyPatient;
		const target = {
			id: "pt-missing-telecom",
			unioned: [{
				tel: "c1",
				sys: "sms"
			}]
		};
		const duckSchema = "{ id: 'VARCHAR', telecom: 'STRUCT(value VARCHAR, system VARCHAR)[]', contact: 'STRUCT(telecom STRUCT(value VARCHAR, system VARCHAR)[])[]', resourceType: 'VARCHAR' }";
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource, duckSchema);
		expect(result).toEqual(target);
	});

	test("_forEach ", async () => {
		const fp = "contact.address._forEach(_col('zip', postalCode), _col('is_patient', false))";
		const resource = unionPatient;
		const target = [{is_patient: false, zip: "z2"}];
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("_splitPath", async() => {
		const fp = "link.other.reference._splitPath(-1)";
		const resource = simplePatient;
		const target = ["456"];
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("function on nested structs", async() => {
		const fp = "subject.reference._splitPath(-1)";
		const resource = simpleObservation;
		const target = "456";
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("_col_collection should return multi-item collections", async() => {
		const fp = "_forEach(_col_collection('name', name))";
		const resource = multipleNames;
		const target = 	{
			name: [{
				use: "official",family: "f1", given: null
			},{ 
				use: "nickname", family: "f2", given: ["g1", "g2"]
			}]
		};
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("_col should fail at runtime if a multi-item collection is returned", async() => {
		const fp = "_forEach(_col('name', name))";
		const resource = multipleNames;
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		expect( async () => {
			await testQuery(query, resource);
		}).toThrow();
	});

	test("_col should pass at runtime if collection with single item is returned", async() => {
		const fp = "_forEach(_col('address', address))";
		const resource = unionPatient;
		const target = {address: {postalCode: "z1"}};
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

});

describe("_invoke function", () => {

	test("_invoke on scalar value", async () => {
		const fp = "id._invoke('upper')";
		const resource = simplePatient;
		const target = "ID-123";
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toBe(target);
	});

	test("_invoke on array value - maps over each element", async () => {
		const fp = "name.family._invoke('upper')";
		const resource = multipleNames;
		const target = ["F1", "F2"];
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("_invoke within _forEach on scalar field", async () => {
		const fp = "name._forEach(_col('family_upper', family._invoke('upper')))";
		const resource = multipleNames;
		const target = [{family_upper: "F1"}, {family_upper: "F2"}];
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("_invoke with string parameter", async () => {
		const fp = "id._invoke('concat', 'suffix')";
		const resource = simplePatient;
		const target = "id-123suffix";
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toBe(target);
	});

	test("_invoke with numeric parameters", async () => {
		const fp = "id._invoke('substring', 1, 2)";
		const resource = simplePatient;
		const target = "id";
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toBe(target);
	});

	test("_invoke with multiple parameters", async () => {
		const fp = "id._invoke('replace', '2', 'X')";
		const resource = simplePatient;
		const target = "id-1X3";
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toBe(target);
	});

	test("_invoke with parameters on array value", async () => {
		const fp = "name.family._invoke('concat', '_suffix')";
		const resource = multipleNames;
		const target = ["f1_suffix", "f2_suffix"];
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("_invoke should throw error when path is used as parameter", () => {
		const fp = "name._forEach(_col('concat', family._invoke('concat', use)))";
		const resource = multipleNames;
		expect(() => {
			buildQuery(fp, resource.resourceType, fhirSchema);
		}).toThrow(/_invoke parameter.*must be a scalar literal value/);
	});

	test("_invoke should throw error when path is used in any parameter position", () => {
		const fp = "id._invoke('substring', name.family, 2)";
		const resource = simplePatient;
		expect(() => {
			buildQuery(fp, resource.resourceType, fhirSchema);
		}).toThrow(/_invoke parameter.*must be a scalar literal value/);
	});

	test("_invoke inside where() on scalar field", async () => {
		const fp = "name.where(family._invoke('upper') = 'F1')";
		const resource = multipleNames;
		const target = [{use: "official", family: "f1", given: null}];
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("_invoke on array before where()", async () => {
		const fp = "name.family._invoke('upper').where($this = 'F1')";
		const resource = multipleNames;
		const target = ["F1"];
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

	test("_invoke on $this in where", async () => {
		const fp = "name.family.where($this._invoke('upper') = 'F1')";
		const resource = multipleNames;
		const target = ["f1"];
		const query = buildQuery(fp, resource.resourceType, fhirSchema);
		const result = await testQuery(query, resource);
		expect(result).toEqual(target);
	});

});
